-- Tags: no-random-merge-tree-settings, no-random-settings
-- EXPLAIN output may differ

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
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, optimize_aggregation_in_order = 0
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Aggregation in order enabled, takes precedence over sharded aggregation';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, optimize_aggregation_in_order = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'No GROUP BY keys';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT sum(b) FROM test_sharded_agg_neg
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'UInt8 key (too low cardinality for sharding)';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT u8, sum(b) FROM test_sharded_agg_neg GROUP BY u8
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Int8 key (too low cardinality for sharding)';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT toInt8(u8) AS k, sum(b) FROM test_sharded_agg_neg GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'LowCardinality key';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT lc_key, sum(b) FROM test_sharded_agg_neg GROUP BY lc_key
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'GROUPING SETS';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE
    SELECT sum(b) AS s
    FROM test_sharded_agg_neg
    GROUP BY GROUPING SETS ((a), (u8))
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'In-order aggregation (force_aggregation_in_order)';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, force_aggregation_in_order = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Single stream (max_threads = 1)';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, max_threads = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Single stream from upstream (max_streams_for_merge_tree_reading = 1)';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, max_streams_for_merge_tree_reading = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Empty table (num_streams = 1)';
DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (a String, b UInt64) ENGINE = MergeTree ORDER BY tuple();
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_empty GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';
DROP TABLE test_empty;

SELECT 'max_rows_to_group_by';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, max_rows_to_group_by = 10, group_by_overflow_mode = 'any'
) WHERE explain LIKE '%ShardByHashTransform%';
