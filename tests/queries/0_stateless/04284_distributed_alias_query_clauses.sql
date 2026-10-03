-- Tags: distributed

-- Forward-looking coverage: alias columns referenced in query clauses
-- other than the SELECT projection. Each section produces a result; the
-- Distributed query must match the equivalent local query.

-- ====================================================================
-- Section 1: WHERE + GROUP BY on alias columns sharing sub-expression
-- ====================================================================
SELECT '---- where_and_group_by ----';
DROP TABLE IF EXISTS t_local_04284_wg;
DROP TABLE IF EXISTS t_dist_04284_wg;

CREATE TABLE t_local_04284_wg
(
  id UInt32,
  f UInt8,
  a UInt8 ALIAS bitAnd(f, 1),
  b UInt8 ALIAS bitAnd(f, 2)
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_local_04284_wg VALUES (1, 1), (2, 3), (3, 2), (4, 0);

CREATE TABLE t_dist_04284_wg AS t_local_04284_wg
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_local_04284_wg, rand());

SELECT 'local';
SELECT a, b, count() AS c FROM t_local_04284_wg WHERE a = 1 GROUP BY a, b ORDER BY a, b;

SELECT 'dist';
SELECT a, b, count() AS c FROM t_dist_04284_wg WHERE a = 1 GROUP BY a, b ORDER BY a, b
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, enable_alias_marker = 1;

SELECT 'dist_plan';
SELECT a, b, count() AS c FROM t_dist_04284_wg WHERE a = 1 GROUP BY a, b ORDER BY a, b
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, serialize_query_plan = 1, enable_alias_marker = 1;

DROP TABLE t_dist_04284_wg;
DROP TABLE t_local_04284_wg;

-- ====================================================================
-- Section 2: JOIN ON references an alias column on one side
-- ====================================================================
SELECT '---- join_on_alias_column ----';
DROP TABLE IF EXISTS t_left_04284;
DROP TABLE IF EXISTS t_right_04284;
DROP TABLE IF EXISTS dist_left_04284;
DROP TABLE IF EXISTS dist_right_04284;

CREATE TABLE t_left_04284
(
  id UInt32,
  x UInt32,
  a UInt32 ALIAS x + 100
)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE t_right_04284
(
  id UInt32,
  a UInt32
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left_04284 VALUES (1, 5), (2, 7), (3, 9);
INSERT INTO t_right_04284 VALUES (10, 105), (20, 107), (30, 120);

CREATE TABLE dist_left_04284 AS t_left_04284
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_left_04284, rand());
CREATE TABLE dist_right_04284 AS t_right_04284
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_right_04284, rand());

SELECT 'local';
SELECT l.id, l.a, r.id FROM t_left_04284 AS l
INNER JOIN t_right_04284 AS r ON l.a = r.a
ORDER BY l.id;

SELECT 'dist';
-- GLOBAL JOIN materializes the right side at the initiator and broadcasts it to
-- each shard, so the JOIN happens locally on each shard. With 2 shards reading
-- the same left data, each shard produces 2 join rows; DISTINCT dedups to
-- the same 2 rows as the local oracle.
SELECT DISTINCT l.id, l.a, r.id FROM dist_left_04284 AS l
GLOBAL INNER JOIN dist_right_04284 AS r ON l.a = r.a
ORDER BY l.id
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, enable_alias_marker = 1;

DROP TABLE dist_left_04284;
DROP TABLE dist_right_04284;
DROP TABLE t_left_04284;
DROP TABLE t_right_04284;

-- ====================================================================
-- Section 3: aggregate over alias column appears only in ORDER BY (not in SELECT)
-- ====================================================================
-- The shard processes at with_mergeable_state_after_aggregation_and_limit
-- and emits a sort-key aggregate state column that the SELECT projection
-- does not request. Naming must agree across the 2-shard merge so the
-- initiator can sort and finalize - exercises the marker's role as the
-- action-node-name carrier inside aggregate function calls that travel
-- in the merge state but not in the user-visible output.
SELECT '---- order_by_sum_alias_col_2_shards ----';
DROP TABLE IF EXISTS t_local_04284_agg;
DROP TABLE IF EXISTS t_dist_04284_agg;

CREATE TABLE t_local_04284_agg
(
  id UInt32,
  f UInt8,
  alias_col UInt8 ALIAS bitAnd(f, 1)
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_local_04284_agg VALUES (1, 1), (1, 3), (2, 2), (2, 0), (3, 7);

-- 2-shard cluster: both shards point at the same local table. Each shard
-- computes its own per-id sum(alias_col), and the initiator merges across
-- the 2 shard states. The merge column ordering is consistent (sum scales
-- by 2), so the projected `id` order is the same as local.
CREATE TABLE t_dist_04284_agg AS t_local_04284_agg
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_local_04284_agg, rand());

SELECT 'local';
SELECT id FROM t_local_04284_agg GROUP BY id ORDER BY sum(alias_col), id;

SELECT 'dist';
SELECT id FROM t_dist_04284_agg GROUP BY id ORDER BY sum(alias_col), id LIMIT 10
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, enable_alias_marker = 1;

SELECT 'dist_plan';
SELECT id FROM t_dist_04284_agg GROUP BY id ORDER BY sum(alias_col), id LIMIT 10
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1, serialize_query_plan = 1, enable_alias_marker = 1;

DROP TABLE t_dist_04284_agg;
DROP TABLE t_local_04284_agg;
