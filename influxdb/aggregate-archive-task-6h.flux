// Aggregate the usage data from fine-grained usage data
// into 5 minute data points with a time weighted average
// (integral of the consumption over the time period).
option task = {
	name: "aggregate-archive-task-6h",
	every: 6h,
	offset: 0m,
	concurrency: 1,
}

data = from(bucket: "iobroker")
	|> range(start: -6h)
	|> filter(fn: (r) =>
		(r["from"] == "system.adapter.shelly.0"))

data
	|> window(every: 5m)
	|> timeWeightedAvg(unit: 5m)
	|> duplicate(column: "_stop", as: "_time")
	|> timeShift(duration: -150s, columns: ["_time"])
	|> window(every: inf)
	|> filter(fn: (r) =>
		(exists r._value))
	|> to(bucket: "energy")
