SELECT quantileSketch(0.0003, 0.9)(number)
FROM
(
    SELECT number
    FROM numbers(100000000)
)

SELECT quantileSketchState(0.0003, 0.9)(number) as s
FROM
(
    SELECT number
    FROM numbers(100000000)
)

SELECT quantileSketchMerge(0.0003, 0.9)(s) FROM(
    SELECT quantileSketchState(0.0003, 0.9)(number) as s
    FROM
    (
        SELECT number
        FROM numbers(100000000)
    )
)

CREATE TABLE t
(
    id UInt64,
    s AggregateFunction(quantileSketch(0.0003, 0.9), Float64)
) ENGINE = MergeTree ORDER BY id



INSERT INTO t
SELECT 1 as id, quantileSketchState(0.0003, 0.9)(number) as s
FROM
(
    SELECT toFloat64(number) as number
    FROM numbers(100)
)




INSERT INTO t
SELECT 2 as id, quantileSketchState(0.02, 0.9)(number) as s
FROM
(
    SELECT toFloat64(number + 100) as number
    FROM numbers(100)
)


SELECT quantileSketchMerge(0.0003, 0.9)(s) FROM t;



CREATE TABLE default.comparision
(
    `id` UInt64,
    `sketch` AggregateFunction(quantileSketch(0.0003, 0.9), UInt64),
    `td` AggregateFunction(quantilesTDigest(0.5, 0.9, 0.99), UInt64),
    `bf` AggregateFunction(quantilesBFloat16(0.5, 0.9, 0.99), UInt64),
)
ENGINE = SummingMergeTree()
ORDER BY id
SETTINGS index_granularity = 8192


insert into comparision
select
    number % 10000 as id,
    quantileSketchState(0.0003, 0.9)(number) as sketch,
    quantilesTDigestState(0.5, 0.9, 0.99)(number) as td,
    quantilesBFloat16State(0.5, 0.9, 0.99)(number) as bf  
from numbers(100000000)
group by id;


SELECT quantileSketchMerge(0.0003, 0.9)(sketch) as sketch, quantileTDigestMerge(0.9)(td) as td, quantileBFloat16Merge(0.9)(bf) as bf FROM comparision;


SELECT
    quantileSketchMerge(0.0003, 0.9)(sketch) AS sketch
FROM comparision

SELECT
    quantileTDigestMerge(0.9)(td) AS td
FROM comparision

SELECT
    quantileBFloat16Merge(0.9)(bf) AS bf
FROM comparision






CREATE TABLE comparision_100mil
(
    `id` UInt64,
    `sketch` AggregateFunction(quantileSketch(0.001, 0.9), UInt64),
    `td` AggregateFunction(quantileTDigest(0.9), UInt64),
    `bf` AggregateFunction(quantileBFloat16(0.9), UInt64),
)
ENGINE = SummingMergeTree()
ORDER BY id
SETTINGS index_granularity = 8192


insert into comparision_100mil
select
    number % 200000 as id,
    quantileSketchState(0.0003, 0.9)(number) as sketch,
    quantileTDigestState(0.9)(number) as td,
    quantileBFloat16State(0.9)(number) as bf  
from numbers(100000000)
group by id;


SELECT quantileSketchMerge(0.0003, 0.95)(sketch) AS sketch
FROM comparision

Query id: a7064e1e-b5c0-4155-b2d8-967fc813098e

┌────────────sketch─┐
│ 95111682.80725858 │
└───────────────────┘

1 row in set. Elapsed: 0.072 sec. Processed 10.00 thousand rows, 480.00 KB (139.76 thousand rows/s., 6.71 MB/s.)
Peak memory usage: 44.02 MiB.

SELECT quantileTDigestMerge(0.95)(td) AS td
FROM comparision

Query id: a052a6a1-d1f5-4abc-af6b-dc4f1a328150

┌───────td─┐
│ 94997150 │
└──────────┘

1 row in set. Elapsed: 0.183 sec. Processed 10.00 thousand rows, 1.36 MB (54.66 thousand rows/s., 7.43 MB/s.)
Peak memory usage: 40.00 MiB.

SELECT quantileBFloat16Merge(0.95)(bf) AS bf
FROM comparision

Query id: 4859585d-1ff7-47e8-ab99-8a0d00f8068e

┌───────bf─┐
│ 94896128 │
└──────────┘

1 row in set. Elapsed: 0.296 sec. Processed 10.00 thousand rows, 4.48 MB (33.74 thousand rows/s., 15.12 MB/s.)
Peak memory usage: 474.73 MiB.