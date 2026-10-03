-- Tags: distributed

-- Regression: the `WITH CLUSTER` stage cap in `StorageDistributed::getQueryProcessingStage`
-- used to return before the `optimize_skip_unused_shards` block, so
-- `query_info.optimized_cluster` was never populated (queries fanned out to all
-- shards) and `force_optimize_skip_unused_shards` enforcement never threw.

SET enable_analyzer = 1;
SET allow_experimental_group_by_with_cluster = 1;

DROP TABLE IF EXISTS t_local_04700;
DROP TABLE IF EXISTS t_dist_04700;

CREATE TABLE t_local_04700 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dist_04700 (k UInt64, v UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_local_04700, k);

-- k values: 0, 1, 2, 3 twice each.
INSERT INTO t_local_04700 SELECT number % 4, number FROM numbers(8);

SET optimize_skip_unused_shards = 1;

-- Both shards of `test_cluster_two_shards_localhost` read the same local table,
-- so a query that is NOT pruned to a single shard counts every row twice.
-- With pruning intact the count is 2; without it, 4.
SELECT sum(c) FROM (SELECT k, count() AS c FROM t_dist_04700 WHERE k = 2 GROUP BY k WITH CLUSTER 0);
SELECT sum(c) FROM (SELECT k, count() AS c FROM t_dist_04700 WHERE k = 3 GROUP BY k WITH CLUSTER 1);

-- `force_optimize_skip_unused_shards` must still throw for `WITH CLUSTER`
-- queries when the WHERE clause gives no sharding-key restriction.
SET force_optimize_skip_unused_shards = 1;
SELECT k, count() FROM t_dist_04700 GROUP BY k WITH CLUSTER 0; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

-- And must still pass when pruning succeeds.
SELECT sum(c) FROM (SELECT k, count() AS c FROM t_dist_04700 WHERE k = 2 GROUP BY k WITH CLUSTER 0);

DROP TABLE t_dist_04700;
DROP TABLE t_local_04700;
