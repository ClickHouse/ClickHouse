-- StorageDistributed::getOptimizedQueryProcessingStageAnalyzer can return the Complete stage (skipping
-- the coordinator merge) when optimize_skip_unused_shards and optimize_distributed_group_by_sharding_key
-- are set and the query's GROUP BY columns match the underlying table's sharding key by name. Under the
-- trivial-view pushdown, a view that rewrites the sharding-key column but keeps its name
-- (SELECT intDiv(id, 2) AS id FROM dist, sharded by id) would fool that name comparison: the view-output
-- group can span multiple shards, so skipping the merge returns per-shard partial groups instead of one
-- merged group. The pushdown disables optimize_distributed_group_by_sharding_key for the underlying read
-- to force the merge, so the result must be correct and identical with the setting on and off.
--
-- test_cluster_two_shards_localhost reads the local table once per shard, so the two (id=10, id=11) rows
-- both map to view id = intDiv(id, 2) = 5 on each shard; the correct global aggregate is a single row.
--
-- Tags: shard

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS data_04374;
DROP TABLE IF EXISTS dist_04374;
DROP VIEW IF EXISTS view_04374;

CREATE TABLE data_04374 (id UInt32, value UInt32) ENGINE = MergeTree() ORDER BY id;
INSERT INTO data_04374 VALUES (10, 100), (11, 200);

CREATE TABLE dist_04374 AS data_04374
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), data_04374, id);

-- The view rewrites the sharding-key column `id` to intDiv(id, 2) but keeps the name `id`.
CREATE VIEW view_04374 AS SELECT intDiv(id, 2) AS id, value FROM dist_04374;

-- Pushdown ON: the merge must not be skipped; a single merged group (5, 600) is returned.
SELECT id, sum(value) FROM view_04374 GROUP BY id ORDER BY id
SETTINGS optimize_trivial_view_pushdown_to_distributed = 1, optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1;

-- Pushdown OFF: same result (reference).
SELECT id, sum(value) FROM view_04374 GROUP BY id ORDER BY id
SETTINGS optimize_trivial_view_pushdown_to_distributed = 0, optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1;

DROP VIEW view_04374;
DROP TABLE dist_04374;
DROP TABLE data_04374;
