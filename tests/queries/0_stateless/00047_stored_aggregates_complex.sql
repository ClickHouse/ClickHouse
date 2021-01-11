DROP TABLE IF EXISTS stored_aggregates;

CREATE TABLE stored_aggregates
(
	d	Date,
	k1 	UInt64,
	k2 	String,
	Sum 		AggregateFunction(sum, UInt64),
	Avg 		AggregateFunction(avg, UInt64),
	Uniq 		AggregateFunction(uniq, UInt64),
	Any 		AggregateFunction(any, String),
	AnyIf 		AggregateFunction(anyIf, String, UInt8),
	Quantiles 	AggregateFunction(quantiles(0.5, 0.9), UInt64),
	GroupArray	AggregateFunction(groupArray, String)
)
ENGINE = AggregatingMergeTree(d, (d, k1, k2), 8192);

INSERT INTO stored_aggregates
SELECT
	toDate('2014-06-01') AS d,
	intDiv(number, 100) AS k1,
	toString(intDiv(number, 10)) AS k2,
	sumState(number) AS Sum,
	avgState(number) AS Avg,
	uniqState(toUInt64(number % 7)) AS Uniq,
	anyState(toString(number)) AS Any,
	anyIfState(toString(number), number % 7 = 0) AS AnyIf,
	quantilesState(0.5, 0.9)(number) AS Quantiles,
	groupArrayState(toString(number)) AS GroupArray
FROM
(
	SELECT * FROM system.numbers LIMIT 1000
)
GROUP BY d, k1, k2
ORDER BY d, k1, k2;

SELECT d, k1, k2,
	sumMerge(Sum), avgMerge(Avg), uniqMerge(Uniq),
	anyMerge(Any), anyIfMerge(AnyIf),
	quantilesMerge(0.5, 0.9)(Quantiles),
	groupArrayMerge(GroupArray)
FROM stored_aggregates
GROUP BY d, k1, k2
ORDER BY d, k1, k2;

SELECT d, k1,
	sumMerge(Sum), avgMerge(Avg), uniqMerge(Uniq),
	anyMerge(Any), anyIfMerge(AnyIf),
	quantilesMerge(0.5, 0.9)(Quantiles),
	groupArrayMerge(GroupArray)
FROM stored_aggregates
GROUP BY d, k1
ORDER BY d, k1;

SELECT d,
	sumMerge(Sum), avgMerge(Avg), uniqMerge(Uniq),
	anyMerge(Any), anyIfMerge(AnyIf),
	quantilesMerge(0.5, 0.9)(Quantiles),
	groupArrayMerge(GroupArray)
FROM stored_aggregates
GROUP BY d
ORDER BY d;

DROP TABLE stored_aggregates;
