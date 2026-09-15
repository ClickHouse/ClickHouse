-- `grouping()` over a `Distributed` table with `optimize_skip_unused_shards`: the query is sent to the
-- shards up to `WithMergeableStateAfterAggregation`, so a shard computes the grouping column itself.
-- The constant index mask that `GroupingFunctionsResolvePass` adds as an argument of
-- `__groupingOrdinary` has to be named on the shard exactly as the initiator names it, or the
-- initiator does not find the column in the block it receives.

DROP TABLE IF EXISTS t_05195;
DROP TABLE IF EXISTS t_05195_dist;
CREATE TABLE t_05195 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05195 SELECT number, 'v' FROM numbers(100);
CREATE TABLE t_05195_dist (id UInt64, value String) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_05195, id);

-- The shards have to finalize the aggregation for the grouping column to be computed there, which is
-- what `optimize_distributed_group_by_sharding_key` does for a `GROUP BY` on the sharding key, and the
-- query has to actually go over the network. Both are pinned here so that randomized settings cannot
-- silently turn this into a different case: with the sharding-key aggregation off, the initiator merges
-- the groups of both shards and the `count()` below is 2 instead of 1.
SET optimize_distributed_group_by_sharding_key = 1, prefer_localhost_replica = 0;

SELECT 'the sharding key';
SELECT grouping(id), id FROM t_05195_dist GROUP BY id ORDER BY id LIMIT 1;
SELECT grouping(id), id FROM t_05195_dist GROUP BY id ORDER BY id LIMIT 1 SETTINGS optimize_skip_unused_shards = 1;
SELECT grouping(id), count() FROM t_05195_dist GROUP BY id ORDER BY id LIMIT 1 SETTINGS optimize_skip_unused_shards = 1;
SELECT grouping(id), id FROM t_05195_dist WHERE id IN (5, 30) GROUP BY id ORDER BY id SETTINGS optimize_skip_unused_shards = 1;

SELECT 'another column';
SELECT grouping(value) FROM t_05195_dist GROUP BY value ORDER BY 1 LIMIT 1 SETTINGS optimize_skip_unused_shards = 1;

SELECT 'the ROLLUP and GROUPING SETS forms';
SELECT grouping(id), grouping(value) FROM t_05195_dist GROUP BY ROLLUP(id, value) ORDER BY 1, 2 LIMIT 1 SETTINGS optimize_skip_unused_shards = 1;
SELECT grouping(id), grouping(value) FROM t_05195_dist GROUP BY GROUPING SETS ((id), (value)) ORDER BY 1, 2 LIMIT 1 SETTINGS optimize_skip_unused_shards = 1;

SELECT 'the cluster table function';
SELECT grouping(id) FROM cluster(test_cluster_two_shards, currentDatabase(), t_05195) GROUP BY id ORDER BY 1 LIMIT 1 SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE t_05195_dist;
DROP TABLE t_05195;
