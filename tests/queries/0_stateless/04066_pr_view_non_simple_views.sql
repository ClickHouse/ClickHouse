-- Test that non-simple views (with GROUP BY, ORDER BY, DISTINCT, LIMIT, or WINDOW) are not
-- eligible for the parallel_replicas_allow_view_over_mergetree optimization.
-- For these views, only the view's inner query is sent for execution on remote nodes
-- with parallel replicas (the standard parallel replicas path), not the outer query
-- over the view.

DROP TABLE IF EXISTS t_base;
DROP TABLE IF EXISTS t_base2;
DROP VIEW IF EXISTS v_simple;
DROP VIEW IF EXISTS v_group_by;
DROP VIEW IF EXISTS v_order_by;
DROP VIEW IF EXISTS v_distinct;
DROP VIEW IF EXISTS v_limit;
DROP VIEW IF EXISTS v_offset;
DROP VIEW IF EXISTS v_limit_by;
DROP VIEW IF EXISTS v_window;
DROP VIEW IF EXISTS v_window_named;
DROP VIEW IF EXISTS v_window_partition;
DROP VIEW IF EXISTS v_union_distinct;

CREATE TABLE t_base (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key SETTINGS index_granularity=1;
CREATE TABLE t_base2 (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key SETTINGS index_granularity=1;
INSERT INTO t_base SELECT number, number * 10 FROM numbers(1000);
INSERT INTO t_base2 SELECT number, number * 10 FROM numbers(1000);

CREATE VIEW v_simple AS SELECT * FROM t_base;
CREATE VIEW v_group_by AS SELECT key % 10 AS k, sum(value) AS s FROM t_base GROUP BY k;
CREATE VIEW v_order_by AS SELECT * FROM t_base ORDER BY key;
CREATE VIEW v_distinct AS SELECT DISTINCT key FROM t_base;
CREATE VIEW v_limit AS SELECT * FROM t_base LIMIT 100;
CREATE VIEW v_offset AS SELECT * FROM t_base OFFSET 100;
CREATE VIEW v_limit_by AS SELECT * FROM t_base LIMIT 1 BY key % 10;
CREATE VIEW v_window AS SELECT key, value, row_number() OVER (ORDER BY key) AS rn FROM t_base;
CREATE VIEW v_window_named AS SELECT key, value, row_number() OVER w AS rn FROM t_base WINDOW w AS (ORDER BY key);
CREATE VIEW v_window_partition AS SELECT key, value, sum(value) OVER (PARTITION BY key % 10) AS s FROM t_base;
CREATE VIEW v_union_distinct AS SELECT key, value FROM t_base UNION DISTINCT SELECT key, value FROM t_base2;

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- For a simple view, ReadFromRemoteParallelReplicas references the view name (v_simple)
-- because the outer query over the view is sent to replicas.
SELECT '-- simple view: query sent over view';
SELECT if(explain LIKE '%v_simple%', 'v_simple', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT key, sum(value) FROM v_simple
    GROUP BY key
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

-- For non-simple views, ReadFromRemoteParallelReplicas references t_base (the underlying table)
-- because the view's inner query is what gets sent to replicas.
SELECT '-- view with GROUP BY: inner query sent over t_base';
SELECT if(explain LIKE '%v_group_by%', 'v_group_by', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT * FROM v_group_by
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with ORDER BY: inner query sent over t_base';
SELECT if(explain LIKE '%v_order_by%', 'v_order_by', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(value) FROM v_order_by
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with DISTINCT: inner query sent over t_base';
SELECT if(explain LIKE '%v_distinct%', 'v_distinct', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(key) FROM v_distinct
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with LIMIT: inner query sent over t_base';
SELECT if(explain LIKE '%v_limit%', 'v_limit', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(value) FROM v_limit
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with OFFSET: inner query sent over t_base';
SELECT if(explain LIKE '%v_offset%', 'v_offset', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(value) FROM v_offset
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with LIMIT BY: inner query sent over t_base';
SELECT if(explain LIKE '%v_limit_by%', 'v_limit_by', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(value) FROM v_limit_by
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with WINDOW (inline): inner query sent over t_base';
SELECT if(explain LIKE '%v_window%', 'v_window', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(rn) FROM v_window
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with WINDOW (named): inner query sent over t_base';
SELECT if(explain LIKE '%v_window_named%', 'v_window_named', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(rn) FROM v_window_named
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with WINDOW (partition): inner query sent over t_base';
SELECT if(explain LIKE '%v_window_partition%', 'v_window_partition', if(explain LIKE '%t_base%', 't_base', 'other'))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(s) FROM v_window_partition
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view with UNION DISTINCT: inner query sent over t_base/t_base2';
SELECT if(explain LIKE '%v_union_distinct%', 'v_union_distinct', if(explain LIKE '%t_base2%', 't_base2', if(explain LIKE '%t_base%', 't_base', 'other')))
FROM viewExplain('EXPLAIN', '', (
    SELECT sum(value) FROM v_union_distinct
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

-- Verify results are correct (match non-parallel execution)
SELECT '-- correctness check';
SELECT sum(value) AS r1 FROM v_simple
    SETTINGS parallel_replicas_allow_view_over_mergetree = 0;
SELECT sum(value) AS r1 FROM v_simple
    SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

DROP VIEW v_simple;
DROP VIEW v_group_by;
DROP VIEW v_order_by;
DROP VIEW v_distinct;
DROP VIEW v_limit;
DROP VIEW v_offset;
DROP VIEW v_limit_by;
DROP VIEW v_window;
DROP VIEW v_window_named;
DROP VIEW v_window_partition;
DROP VIEW v_union_distinct;
DROP TABLE t_base2;
DROP TABLE t_base;
