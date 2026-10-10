SET max_rows_to_group_by = 0;

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
         SETTINGS enable_sharding_aggregator = 0, optimize_aggregation_in_order = 0))
    =
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS enable_sharding_aggregator = 1, optimize_aggregation_in_order = 0));

SELECT 'Aggregation in order enabled, takes precedence over sharded aggregation';
SELECT
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS enable_sharding_aggregator = 0, optimize_aggregation_in_order = 1))
    =
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS enable_sharding_aggregator = 1, optimize_aggregation_in_order = 1));

SELECT 'No GROUP BY keys';
SELECT
    (SELECT sum(b) FROM test_sharded_agg_neg SETTINGS enable_sharding_aggregator = 0)
    =
    (SELECT sum(b) FROM test_sharded_agg_neg SETTINGS enable_sharding_aggregator = 1);

SELECT 'UInt8 key (too low cardinality for sharding)';
SELECT
    (SELECT sum(s), count() FROM (SELECT u8, sum(b) AS s FROM test_sharded_agg_neg GROUP BY u8 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT u8, sum(b) AS s FROM test_sharded_agg_neg GROUP BY u8 SETTINGS enable_sharding_aggregator = 1));

SELECT 'Int8 key (too low cardinality for sharding)';
SELECT
    (SELECT sum(s), count() FROM (SELECT toInt8(u8) AS k, sum(b) AS s FROM test_sharded_agg_neg GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toInt8(u8) AS k, sum(b) AS s FROM test_sharded_agg_neg GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'LowCardinality key';
SELECT
    (SELECT sum(s), count() FROM (SELECT lc_key, sum(b) AS s FROM test_sharded_agg_neg GROUP BY lc_key SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT lc_key, sum(b) AS s FROM test_sharded_agg_neg GROUP BY lc_key SETTINGS enable_sharding_aggregator = 1));

SELECT 'GROUPING SETS';
SELECT
    (SELECT sum(s), count() FROM
        (SELECT sum(b) AS s
         FROM test_sharded_agg_neg
         GROUP BY GROUPING SETS ((a), (u8))
         SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM
        (SELECT sum(b) AS s
         FROM test_sharded_agg_neg
         GROUP BY GROUPING SETS ((a), (u8))
         SETTINGS enable_sharding_aggregator = 1));

SELECT 'Single stream (max_threads = 1)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a SETTINGS enable_sharding_aggregator = 0, max_threads = 1))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a SETTINGS enable_sharding_aggregator = 1, max_threads = 1));

DROP TABLE test_sharded_agg_neg;
