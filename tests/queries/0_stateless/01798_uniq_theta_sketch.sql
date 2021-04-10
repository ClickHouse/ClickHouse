DROP TABLE IF EXISTS stored_aggregates;

-- simple
CREATE TABLE stored_aggregates
(
    d Date,
    Uniq AggregateFunction(uniq, UInt64),
    UniqThetaSketch AggregateFunction(uniqThetaSketch, UInt64)
)
ENGINE = AggregatingMergeTree(d, d, 8192);

INSERT INTO stored_aggregates
SELECT
    toDate('2014-06-01') AS d,
    uniqState(number) AS Uniq,
    uniqThetaSketchState(number) AS UniqThetaSketch
FROM
(
    SELECT * FROM system.numbers LIMIT 1000
);

SELECT uniqMerge(Uniq), uniqThetaSketchMerge(UniqThetaSketch) FROM stored_aggregates;

SELECT d, uniqMerge(Uniq), uniqThetaSketchMerge(UniqThetaSketch) FROM stored_aggregates GROUP BY d ORDER BY d;

OPTIMIZE TABLE stored_aggregates;

SELECT uniqMerge(Uniq), uniqThetaSketchMerge(UniqThetaSketch) FROM stored_aggregates;

SELECT d, uniqMerge(Uniq), uniqThetaSketchMerge(UniqThetaSketch) FROM stored_aggregates GROUP BY d ORDER BY d;

DROP TABLE stored_aggregates;

-- complex
CREATE TABLE stored_aggregates
(
	d	Date,
	k1 	UInt64,
	k2 	String,
	Uniq 			AggregateFunction(uniq, UInt64),
    UniqThetaSketch	AggregateFunction(uniqThetaSketch, UInt64)
)
ENGINE = AggregatingMergeTree(d, (d, k1, k2), 8192);

INSERT INTO stored_aggregates
SELECT
	toDate('2014-06-01') AS d,
	intDiv(number, 100) AS k1,
	toString(intDiv(number, 10)) AS k2,
	uniqState(toUInt64(number % 7)) AS Uniq,
    uniqThetaSketchState(toUInt64(number % 7)) AS UniqThetaSketch
FROM
(
	SELECT * FROM system.numbers LIMIT 1000
)
GROUP BY d, k1, k2
ORDER BY d, k1, k2;

SELECT d, k1, k2,
	uniqMerge(Uniq), uniqThetaSketchMerge(UniqThetaSketch)
FROM stored_aggregates
GROUP BY d, k1, k2
ORDER BY d, k1, k2;

SELECT d, k1,
	uniqMerge(Uniq), uniqThetaSketchMerge(UniqThetaSketch)
FROM stored_aggregates
GROUP BY d, k1
ORDER BY d, k1;

SELECT d,
	uniqMerge(Uniq), uniqThetaSketchMerge(UniqThetaSketch)
FROM stored_aggregates
GROUP BY d
ORDER BY d;

DROP TABLE stored_aggregates;

---- sum + uniq with more data
drop table if exists summing_merge_tree_null;
drop table if exists summing_merge_tree_aggregate_function;
create table summing_merge_tree_null (
    d materialized today(),
    k UInt64,
    c UInt64,
    u UInt64
) engine=Null;

create materialized view summing_merge_tree_aggregate_function (
    d Date,
    k UInt64,
    c UInt64,
    un AggregateFunction(uniq, UInt64),
    ut AggregateFunction(uniqThetaSketch, UInt64)
) engine=SummingMergeTree(d, k, 8192)
as select d, k, sum(c) as c, uniqState(u) as un, uniqThetaSketchState(u) as ut
from summing_merge_tree_null
group by d, k;

-- prime number 53 to avoid resonanse between %3 and %53
insert into summing_merge_tree_null select number % 3, 1, number % 53 from numbers(999999);

select k, sum(c), uniqMerge(un), uniqThetaSketchMerge(ut) from summing_merge_tree_aggregate_function group by k order by k;
optimize table summing_merge_tree_aggregate_function;
select k, sum(c), uniqMerge(un), uniqThetaSketchMerge(ut) from summing_merge_tree_aggregate_function group by k order by k;

drop table summing_merge_tree_aggregate_function;
drop table summing_merge_tree_null;

-- precise
SELECT uniqExact(number) FROM numbers(1e7);
SELECT uniqCombined(number) FROM numbers(1e7);
SELECT uniqCombined64(number) FROM numbers(1e7);
SELECT uniqThetaSketch(number) FROM numbers(1e7);
