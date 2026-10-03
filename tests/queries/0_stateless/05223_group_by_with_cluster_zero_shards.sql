-- Tags: distributed

-- Regression: the `WITH CLUSTER` stage cap in `StorageDistributed::getQueryProcessingStage`
-- returned `WithMergeableState` even when `optimize_skip_unused_shards` pruned every
-- shard away. `StorageDistributed::read` builds no remote plan for zero shards, the
-- planner substitutes an empty source with the raw storage header, and the initiator
-- then tried to merge "aggregate states" that were plain columns. Zero shards must
-- keep the regular `FetchColumns` answer so the initiator runs the whole pipeline.

SET enable_analyzer = 1;
SET allow_experimental_group_by_with_cluster = 1;
SET optimize_skip_unused_shards = 1;

DROP TABLE IF EXISTS t_local_05223;
DROP TABLE IF EXISTS t_dist_05223;

CREATE TABLE t_local_05223 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dist_05223 (k UInt64, v UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_local_05223, k);

INSERT INTO t_local_05223 SELECT number % 4, number FROM numbers(8);

-- `k = 1 AND k = 2` is a contradiction on the sharding key: every shard is pruned.
SELECT k, count() FROM t_dist_05223 WHERE k = 1 AND k = 2 GROUP BY k WITH CLUSTER 1;
SELECT k, count(), sum(v), uniqExact(v) FROM t_dist_05223 WHERE k = 1 AND k = 2 GROUP BY k WITH CLUSTER 1;
SELECT count() FROM t_dist_05223 WHERE k = 1 AND k = 2 GROUP BY k WITH CLUSTER 1;

-- Computed key and a tuple (2D) key.
SELECT k * 2 AS k2, count() FROM t_dist_05223 WHERE k = 1 AND k = 2 GROUP BY k2 WITH CLUSTER 1;
SELECT (k, v), count() FROM t_dist_05223 WHERE k = 1 AND k = 2 GROUP BY (k, v) WITH CLUSTER 1;

-- String key (Levenshtein metric) and `ORDER BY ... LIMIT` on top of the empty source.
SELECT toString(k) AS s, count() FROM t_dist_05223 WHERE k = 1 AND k = 2 GROUP BY s WITH CLUSTER 1;
SELECT k, count() AS c FROM t_dist_05223 WHERE k = 1 AND k = 2 GROUP BY k WITH CLUSTER 1 ORDER BY c DESC LIMIT 3;

-- The plan must not contain a second-stage merge of aggregate states.
SELECT count() FROM (EXPLAIN SELECT k, count() FROM t_dist_05223 WHERE k = 1 AND k = 2 GROUP BY k WITH CLUSTER 1)
WHERE explain ILIKE '%MergingAggregated%';

-- Sanity check: with a satisfiable filter the same queries still return data.
SELECT k, count() FROM t_dist_05223 WHERE k = 2 GROUP BY k WITH CLUSTER 1;

DROP TABLE t_dist_05223;
DROP TABLE t_local_05223;
