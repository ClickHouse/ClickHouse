DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    `d` Nullable(Date),
    `f1` Nullable(String),
    `f2` Nullable(String),
    `c` Nullable(Int64)
)
ENGINE = ReplacingMergeTree
ORDER BY (f1, f2, d)
SETTINGS allow_nullable_key = 1;

INSERT INTO t SELECT
    toDate('2023-09-10', 'UTC') AS d,
    [number % 99999, NULL][number % 2] AS f1,
    ['x', NULL][number % 2] AS f2,
    [number, NULL][number % 2] AS c
FROM numbers(100000);

SELECT
    date_trunc('month', d),
    SUM(c)
FROM t
FINAL
WHERE f2 = 'x'
GROUP BY 1;

DROP TABLE t;

CREATE TABLE t
(
    `d` Nullable(Date),
    `f1` Nullable(String),
    `f2` Nullable(String),
    `c` Nullable(Int64)
)
ENGINE = SummingMergeTree
ORDER BY (f1, f2, d)
SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO t SELECT
    toDate('2023-09-10', 'UTC') AS d,
    NULL AS f1,
    ['x', 'y', 'z'][number % 3] AS f2,
    number AS c
FROM numbers(1000);

SELECT
    date_trunc('month', d),
    SUM(c)
FROM t
FINAL
WHERE f2 = 'x'
GROUP BY 1;

DROP TABLE t;

CREATE TABLE t (o Nullable(String), p Nullable(String)) ENGINE = ReplacingMergeTree ORDER BY (p, o) SETTINGS allow_nullable_key = 1, index_granularity = 2;

INSERT INTO t SELECT number, NULL FROM numbers(10);

SELECT count() FROM t FINAL;

DROP TABLE t;
