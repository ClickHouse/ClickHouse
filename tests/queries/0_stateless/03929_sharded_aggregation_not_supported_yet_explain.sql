-- Tags: no-random-merge-tree-settings, no-random-settings
-- EXPLAIN output may differ

-- Tests queries that could/should be supported by sharded aggregation but in the early stages of
-- development does not support them for now.

SET max_rows_to_group_by = 0;

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
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, b % 100 AS k, sum(b) FROM test GROUP BY a, k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT '-If combinators (sumIf, countIf)';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sumIf(b, flag), countIf(flag), max(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT '-Array combinator (sumArray)';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sumArray(arr) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT '-State combinator (sumState)';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sumState(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'max_rows_to_group_by';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, max_rows_to_group_by = 10, group_by_overflow_mode = 'any'
) WHERE explain LIKE '%ScatterByHashTransform%';
