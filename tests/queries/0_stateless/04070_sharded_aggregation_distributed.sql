-- Tags: distributed, no-random-settings, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- Sharded aggregation must not activate for distributed queries where the merge
-- phase requires bucket-ordered results (should_produce_results_in_order_of_bucket_number).

SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS test_sharded_agg_dist;
CREATE TABLE test_sharded_agg_dist
(
    a String,
    b UInt64
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test_sharded_agg_dist
SELECT
    toString(rand() % 100000) AS a,
    number AS b
FROM numbers(300000);

SELECT 'Distributed aggregation via remote(): correctness';
SELECT
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM remote('127.0.0.{1,1}', currentDatabase(), test_sharded_agg_dist) GROUP BY a
         SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM remote('127.0.0.{1,1}', currentDatabase(), test_sharded_agg_dist) GROUP BY a
         SETTINGS enable_sharding_aggregator = 1));

SELECT 'Distributed aggregation via remote(): no ShardByHashTransform in pipeline';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM remote('127.0.0.{1,1}', currentDatabase(), test_sharded_agg_dist) GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

DROP TABLE test_sharded_agg_dist;
