-- Test for ORDER BY pushdown optimization into VIEWs over distributed tables.
-- A simple SELECT * style VIEW over a Distributed table should be transparent
-- for ORDER BY/LIMIT, so the underlying Distributed table sees the ORDER BY
-- and can use merge-sorted-streams instead of returning unsorted rows that the
-- coordinator would have to fully sort.

-- Tags: distributed

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_local_04241;
DROP TABLE IF EXISTS test_distributed_04241;
DROP VIEW IF EXISTS test_view_04241;

CREATE TABLE test_local_04241 (
    id UInt64,
    val String,
    ts DateTime
) ENGINE = MergeTree()
ORDER BY (id, ts);

CREATE TABLE test_distributed_04241 AS test_local_04241
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), test_local_04241, id);

INSERT INTO test_local_04241 SELECT number, toString(number), now() - number FROM numbers(100);

CREATE VIEW test_view_04241 AS
SELECT id, val, ts FROM test_distributed_04241;

-- Direct query against the Distributed table uses merge-sorted-streams (baseline).
SELECT 'Direct query has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_distributed_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- View query must also use merge-sorted-streams thanks to the pushdown.
SELECT 'View query has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- EXPLAIN PLAN: after pushdown the merge-sorted-streams step must appear in
-- the plan. Match only the merge step to keep the reference stable across
-- builds (the surrounding plan shape depends on `Distributed` internals).
SELECT trim(replaceRegexpAll(explain, '^\\s+', '')) AS step
FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10)
WHERE explain LIKE '%Merge sorted streams%';

-- Result correctness: pushdown must not change result rows.
SELECT 'View ORDER BY+LIMIT result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10
);

-- ORDER BY ... NULLS FIRST: modifier must survive pushdown.
SELECT 'NULLS FIRST has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC NULLS FIRST LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

SELECT 'NULLS FIRST result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC NULLS FIRST LIMIT 10
);

-- LIMIT ... WITH TIES: pushdown must be disabled (ties are computed globally
-- after ORDER BY, so per-shard truncation could drop tied rows).
SELECT 'WITH TIES disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10 WITH TIES)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'WITH TIES result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10 WITH TIES
);

-- ORDER BY ... WITH FILL: pushdown must be disabled (WITH FILL synthesizes
-- rows; per-shard fills would produce a wrong final set after merging).
-- The query has an outer `LIMIT` so the early `hasLimit()` guard does not
-- short-circuit — this exercises the `WITH FILL` rejection branch directly.
SELECT 'WITH FILL disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY id WITH FILL FROM 0 TO 5 LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- ORDER BY without outer LIMIT: pushdown must be disabled (the outer planner
-- still applies a full ORDER BY on the coordinator and would not see the
-- merge-sorted streams from the inner view, so pushing only adds a redundant
-- per-shard sort with no benefit).
SELECT 'No LIMIT disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'No LIMIT returns rows:',
    (SELECT count() > 0 FROM (SELECT id FROM test_view_04241 ORDER BY ts DESC)) AS has_rows;

-- Outer GROUP BY: pushdown must be disabled (it would otherwise break aggregate
-- projection matching and similar optimizations). The query must still produce
-- correct results.
SELECT 'GROUP BY result count:', count() FROM (
    SELECT id, count() FROM test_view_04241 GROUP BY id ORDER BY id LIMIT 5
);

-- Outer LIMIT ... OFFSET ...: pushdown must be disabled to preserve correctness.
SELECT 'OFFSET result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 5 OFFSET 3
);

-- Outer LIMIT BY: pushdown must be disabled because LIMIT BY is evaluated
-- globally on the coordinator after merging, so per-shard truncation could
-- drop candidates needed to fill each group.
SELECT 'LIMIT BY disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 1 BY id LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'LIMIT BY result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 1 BY id LIMIT 10
);

-- Outer JOIN: pushdown must be disabled because the JOIN may filter or expand
-- rows after per-shard truncation, dropping rows that belong to the global top-N.
DROP TABLE IF EXISTS test_join_04241;
CREATE TABLE test_join_04241 (id UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_join_04241 SELECT number FROM numbers(50);

SELECT 'JOIN disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT v.id FROM test_view_04241 v INNER JOIN test_join_04241 j ON v.id = j.id ORDER BY v.ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'JOIN result count:', count() FROM (
    SELECT v.id FROM test_view_04241 v INNER JOIN test_join_04241 j ON v.id = j.id ORDER BY v.ts DESC LIMIT 10
);

-- Outer WHERE: pushdown must be disabled. `query_info.filter_actions_dag` is
-- only used for analysis (e.g. skip-unused-shards), not as a runtime filter
-- inside the view, so pushing LIMIT would truncate rows before WHERE is
-- applied and could return fewer rows than expected. The result-count check
-- uses a filter `id > 50` combined with `ORDER BY ts DESC` (where `ts` is
-- monotonically decreasing in `id`) so that with buggy pushdown the inner
-- top-10 by `ts` would contain only `id <= 9` and the outer filter would
-- drop them all (0 rows) — the correct behavior must return 10 rows.
SELECT 'WHERE disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 WHERE id > 50 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'WHERE result count:', count() FROM (
    SELECT id FROM test_view_04241 WHERE id > 50 ORDER BY ts DESC LIMIT 10
);

-- `additional_table_filters` setting: pushdown must be disabled. The filter is
-- applied as a separate planner filter above the view subquery (because
-- `StorageView` does not support prewhere), so pushing `LIMIT` would truncate
-- before the filter runs and could return fewer rows than expected. The
-- correctness check uses `id > 50` with `ORDER BY ts DESC` (where `ts` is
-- monotonically decreasing in `id`) so that with buggy pushdown the inner
-- top-10 by `ts` would all have `id <= 9` and be dropped, returning 0 rows —
-- the correct behavior must return 10 rows.
SELECT 'additional_table_filters disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10
                              SETTINGS additional_table_filters = {'test_view_04241':'id > 50'})
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'additional_table_filters result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10
    SETTINGS additional_table_filters = {'test_view_04241':'id > 50'}
);

-- Row policy on the view: pushdown must be disabled. Row policies on the view
-- are applied as a planner `where_filters` step above the view subquery (since
-- `StorageView` does not support prewhere), so pushing `LIMIT` would truncate
-- before the policy filter runs. Same correctness check pattern as above —
-- buggy pushdown would return 0 rows instead of the correct 10.
DROP ROW POLICY IF EXISTS test_policy_04241 ON test_view_04241;
CREATE ROW POLICY test_policy_04241 ON test_view_04241 USING id > 50 TO ALL;

SELECT 'row policy disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'row policy result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10
);

DROP ROW POLICY test_policy_04241 ON test_view_04241;

-- View with a named `WINDOW` clause: pushdown must be disabled. Pushing
-- `ORDER BY/LIMIT` would make each shard compute its window over only the
-- per-shard rows, which is not equivalent to computing the window over the
-- global row set and then taking the top-N.
DROP VIEW IF EXISTS test_view_window_named_04241;
CREATE VIEW test_view_window_named_04241 AS
SELECT id, val, ts, row_number() OVER w AS rn FROM test_distributed_04241 WINDOW w AS (ORDER BY id);

SELECT 'view named WINDOW disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_window_named_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

DROP VIEW test_view_window_named_04241;

-- View with an inline `OVER (...)` window function call: same reasoning as
-- above — pushdown must be disabled because the window's partition/order are
-- evaluated over the per-shard rows after `LIMIT` truncates them.
DROP VIEW IF EXISTS test_view_window_inline_04241;
CREATE VIEW test_view_window_inline_04241 AS
SELECT id, val, ts, row_number() OVER (ORDER BY id) AS rn FROM test_distributed_04241;

SELECT 'view inline OVER disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_window_inline_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

DROP VIEW test_view_window_inline_04241;

-- Outer query with a named `WINDOW` clause: pushdown must be disabled because
-- the named `WINDOW` is evaluated globally after merging, while pushed
-- `ORDER BY/LIMIT` would truncate rows per-shard before the window step.
SELECT 'outer named WINDOW disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT row_number() OVER w AS rn, id FROM test_view_04241 WINDOW w AS (ORDER BY id) ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- Outer query with an inline `OVER (...)` window function call: pushdown must
-- be disabled for the same reason. The earlier `outer->hasWindow()` check only
-- caught the named `WINDOW` form above; this exercises the inline path that is
-- detected via `select_query_info.has_window` (computed by
-- `hasWindowFunctionNodes` over the whole query tree).
SELECT 'outer inline OVER disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT row_number() OVER (ORDER BY id) AS rn, id FROM test_view_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

DROP TABLE test_join_04241;
DROP VIEW test_view_04241;
DROP TABLE test_distributed_04241;
DROP TABLE test_local_04241;
