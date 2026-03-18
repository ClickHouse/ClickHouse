-- Tags: no-random-merge-tree-settings, no-random-settings

DROP TABLE IF EXISTS test_debug;
CREATE TABLE test_debug (a String, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_debug SELECT toString(rand() % 1000) AS a, number AS b FROM numbers(10000);

SELECT 'Setting value:';
SELECT getSetting('optimize_aggregation_by_sharding');

SELECT 'Setting value with explicit override:';
SELECT getSetting('optimize_aggregation_by_sharding') SETTINGS optimize_aggregation_by_sharding = 1;

SELECT 'max_threads value:';
SELECT getSetting('max_threads') SETTINGS max_threads = 8;

SELECT 'Full pipeline:';
EXPLAIN PIPELINE SELECT a, sum(b) FROM test_debug GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, max_threads = 8;

SELECT 'Check ScatterByHashTransform:';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_debug GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, max_threads = 8
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Check ShardedAggregatingTransform:';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_debug GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, max_threads = 8
) WHERE explain LIKE '%ShardedAggregatingTransform%';

DROP TABLE test_debug;
