-- Tests queries that could/should be supported by sharded aggregation but in the early stages of
-- development does not support them for now.

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a String,
    b UInt64,
    u8 UInt8,
    arr Array(UInt64),
    flag UInt8
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test
SELECT
    toString(rand() % 100000) AS a,
    number AS b,
    toUInt8(number % 250) AS u8,
    [number % 3, number % 7, number % 11] AS arr,
    toUInt8(number % 2) AS flag
FROM numbers(3000);

SELECT 'Multi-key GROUP BY';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, b % 100 AS k, sum(b) AS s FROM test GROUP BY a, k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, b % 100 AS k, sum(b) AS s FROM test GROUP BY a, k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT '-If combinators (sumIf, countIf)';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sumIf(b, flag) AS s1, countIf(flag) AS s2, max(b) AS s3
         FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sumIf(b, flag) AS s1, countIf(flag) AS s2, max(b) AS s3
         FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT '-Array combinator (sumArray)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sumArray(arr) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sumArray(arr) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

-- max_rows_to_group_by with 'any' mode is non-deterministic (which keys survive depends
-- on processing order), so correctness comparison is not reliable. The EXPLAIN test verifies
-- that sharded aggregation is not used.

DROP TABLE test;
