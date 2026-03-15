DROP TABLE IF EXISTS test_sharded_agg_neg;
CREATE TABLE test_sharded_agg_neg
(
    a String,
    b UInt64,
    u8 UInt8,
    lc_key LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test_sharded_agg_neg
SELECT
    toString(rand() % 100000) AS a,
    number AS b,
    toUInt8(number % 250) AS u8,
    toLowCardinality(toString(number % 1000)) AS lc_key
FROM numbers(300000);

INSERT INTO test_sharded_agg_neg
SELECT
    toString(rand() % 100000) AS a,
    number AS b,
    toUInt8(number % 250) AS u8,
    toLowCardinality(toString(number % 1000)) AS lc_key
FROM numbers(300000);

SELECT 'Base case: sharded aggregation is used';
SELECT
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS optimize_aggregation_by_sharding = 0, optimize_aggregation_in_order = 0))
    =
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS optimize_aggregation_by_sharding = 1, optimize_aggregation_in_order = 0));

SELECT 'Aggregation in order enabled, takes precedence over sharded aggregation';
SELECT
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS optimize_aggregation_by_sharding = 0, optimize_aggregation_in_order = 1))
    =
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS optimize_aggregation_by_sharding = 1, optimize_aggregation_in_order = 1));

SELECT 'No GROUP BY keys';
SELECT
    (SELECT sum(b) FROM test_sharded_agg_neg SETTINGS optimize_aggregation_by_sharding = 0)
    =
    (SELECT sum(b) FROM test_sharded_agg_neg SETTINGS optimize_aggregation_by_sharding = 1);

-- max_rows_to_group_by with 'any' mode is non-deterministic (which keys survive depends
-- on processing order), so correctness comparison is not reliable. The EXPLAIN test in
-- 03928_sharded_aggregation_negative_explain.sql verifies that sharded aggregation is not used.

SELECT 'LowCardinality key';
SELECT
    (SELECT sum(s), count() FROM (SELECT lc_key, sum(b) AS s FROM test_sharded_agg_neg GROUP BY lc_key SETTINGS optimize_aggregation_by_sharding = 0, max_threads = 8))
    =
    (SELECT sum(s), count() FROM (SELECT lc_key, sum(b) AS s FROM test_sharded_agg_neg GROUP BY lc_key SETTINGS optimize_aggregation_by_sharding = 1, max_threads = 8));

SELECT 'GROUPING SETS';
SELECT
    (SELECT sum(s), count() FROM
        (SELECT sum(b) AS s
         FROM test_sharded_agg_neg
         GROUP BY GROUPING SETS ((a), (u8))
         SETTINGS optimize_aggregation_by_sharding = 0, max_threads = 8))
    =
    (SELECT sum(s), count() FROM
        (SELECT sum(b) AS s
         FROM test_sharded_agg_neg
         GROUP BY GROUPING SETS ((a), (u8))
         SETTINGS optimize_aggregation_by_sharding = 1, max_threads = 8));

SELECT 'Single stream (max_threads = 1)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0, max_threads = 1))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1, max_threads = 1));

DROP TABLE test_sharded_agg_neg;
