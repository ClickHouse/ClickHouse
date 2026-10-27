-- Test that a view with UNION ALL is not eligible for the
-- parallel_replicas_allow_view_over_mergetree optimization when:
-- 1) some branches reference non-MergeTree tables
-- 2) multiple branches reference the same MergeTree table
-- When ineligible, ReadFromRemoteParallelReplicas step references the underlying
-- table (inner query path), not the view name.

DROP TABLE IF EXISTS t_mt1;
DROP TABLE IF EXISTS t_mt2;
DROP TABLE IF EXISTS t_mem;
DROP VIEW IF EXISTS v_mixed;
DROP VIEW IF EXISTS v_same_mt;
DROP VIEW IF EXISTS v_diff_mt;

CREATE TABLE t_mt1 (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key SETTINGS index_granularity=1;
CREATE TABLE t_mt2 (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key SETTINGS index_granularity=1;
CREATE TABLE t_mem (key UInt64, value UInt64) ENGINE = Memory;

INSERT INTO t_mt1 SELECT number, number * 10 FROM numbers(100);
INSERT INTO t_mt2 SELECT number + 100, number * 20 FROM numbers(100);
INSERT INTO t_mem SELECT number + 200, number * 30 FROM numbers(100);

-- View with UNION ALL: one MergeTree branch + one Memory branch -> ineligible.
CREATE VIEW v_mixed AS
SELECT key, value FROM t_mt1
UNION ALL
SELECT key, value FROM t_mem;

-- View with UNION ALL: same MergeTree table in both branches -> ineligible
-- (would cause duplicate replica announcements).
CREATE VIEW v_same_mt AS
SELECT key, value FROM t_mt1
UNION ALL
SELECT key, value FROM t_mt1;

-- View with UNION ALL: two different MergeTree tables -> eligible.
CREATE VIEW v_diff_mt AS
SELECT key, value FROM t_mt1
UNION ALL
SELECT key, value FROM t_mt2;

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- Mixed view (MergeTree + Memory): NOT eligible.
SELECT '-- mixed union: inner query sent over t_mt1';
SELECT if(explain LIKE '%v_mixed%', 'v_mixed', if(explain LIKE '%t_mt1%', 't_mt1', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(value) FROM v_mixed
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

-- Same table twice (MergeTree + same MergeTree): NOT eligible.
SELECT '-- same table union: inner query sent over t_mt1';
SELECT if(explain LIKE '%v_same_mt%', 'v_same_mt', if(explain LIKE '%t_mt1%', 't_mt1', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(value) FROM v_same_mt
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

-- Different MergeTree tables: eligible.
SELECT '-- diff mergetree union: query sent over view';
SELECT if(explain LIKE '%v_diff_mt%', 'v_diff_mt', if(explain LIKE '%t_mt%', 't_mt', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(value) FROM v_diff_mt
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

-- Correctness checks.
SELECT '-- correctness: mixed view';
SELECT sum(value) FROM v_mixed SETTINGS parallel_replicas_allow_view_over_mergetree = 0;
SELECT sum(value) FROM v_mixed SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

SELECT '-- correctness: same table view';
SELECT sum(value) FROM v_same_mt SETTINGS parallel_replicas_allow_view_over_mergetree = 0;
SELECT sum(value) FROM v_same_mt SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

SELECT '-- correctness: diff mergetree view';
SELECT sum(value) FROM v_diff_mt SETTINGS parallel_replicas_allow_view_over_mergetree = 0;
SELECT sum(value) FROM v_diff_mt SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

DROP VIEW v_diff_mt;
DROP VIEW v_same_mt;
DROP VIEW v_mixed;
DROP TABLE t_mem;
DROP TABLE t_mt2;
DROP TABLE t_mt1;
