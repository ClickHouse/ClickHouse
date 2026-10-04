-- When a trivial view rewrites the sharding-key column but keeps its name, the pushdown must not
-- let optimize_skip_unused_shards prune using the view-namespace filter against the underlying
-- table's sharding key. filter_actions_dag is built from the view's output columns; after inlining
-- it is cleared so shard pruning is skipped rather than applied to a stale predicate.
--
-- dist is sharded by raw `id`; view exposes `id + 1 AS id`. `WHERE id = 10` means raw id = 9, which
-- is stored on the shard that value 9 routes to (shard_1). The stale filter `id = 10` would prune to
-- the shard for value 10 (shard_0, empty), dropping the row. The query must return 10 on both the
-- optimized and non-optimized paths.
--
-- Tags: distributed, no-parallel

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;

DROP TABLE IF EXISTS shard_0.t04369;
DROP TABLE IF EXISTS shard_1.t04369;
DROP TABLE IF EXISTS t04369_dist;
DROP VIEW IF EXISTS t04369_view;

CREATE TABLE shard_0.t04369 (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t04369 (id UInt32) ENGINE = MergeTree ORDER BY id;

-- raw id = 9 (→ view id = 10) lives only on shard_1; shard_0 (where filter value 10 routes) is empty.
INSERT INTO shard_1.t04369 VALUES (9);

CREATE TABLE t04369_dist (id UInt32)
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t04369, id);

CREATE VIEW t04369_view AS SELECT id + 1 AS id FROM t04369_dist;

-- Pushdown ON: must read the shard holding raw id = 9 and return 10 (shard pruning must not drop it).
SELECT id FROM t04369_view WHERE id = 10
SETTINGS optimize_trivial_view_pushdown_to_distributed = 1, optimize_skip_unused_shards = 1;

-- Pushdown OFF: same result (reference).
SELECT id FROM t04369_view WHERE id = 10
SETTINGS optimize_trivial_view_pushdown_to_distributed = 0, optimize_skip_unused_shards = 1;

DROP VIEW t04369_view;
DROP TABLE t04369_dist;
DROP TABLE shard_0.t04369;
DROP TABLE shard_1.t04369;
