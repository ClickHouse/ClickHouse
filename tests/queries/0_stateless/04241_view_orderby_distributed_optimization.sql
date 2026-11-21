-- Test for ORDER BY pushdown optimization into VIEWs over distributed tables.
-- A simple SELECT * style VIEW over a Distributed table should be transparent
-- for ORDER BY/LIMIT, so the underlying Distributed table sees the ORDER BY
-- and can use merge-sorted-streams instead of returning unsorted rows that the
-- coordinator would have to fully sort.

-- Tags: distributed, no-parallel
-- ^ no-parallel: the SQL SECURITY DEFINER cases below create globally-named users,
-- which would collide across concurrent runs (e.g. the flaky check).

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_local_04241;
DROP TABLE IF EXISTS test_distributed_04241;
DROP VIEW IF EXISTS test_view_04241;

-- The `SQL SECURITY DEFINER` views below depend on the `definer_*_04241` users,
-- so a leftover view from a partially-failed previous run would make a later
-- `DROP USER` throw `HAVE_DEPENDENT_OBJECTS`. Drop the definer views before the
-- users (here and again before `DROP USER` below) to keep the test rerunnable.
DROP VIEW IF EXISTS test_view_definer_limit_04241;
DROP VIEW IF EXISTS test_view_definer_offset_04241;
DROP VIEW IF EXISTS test_view_definer_alias_04241;

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
-- Strip both leading whitespace and the pretty tree-drawing prefix
-- (`explain_query_plan_default = 'pretty'` is the default and decorates each
-- step with characters like `└──`/`├──`/`│`), so the reference stays stable
-- regardless of the step's depth and position in the plan tree.
SELECT trim(replaceRegexpAll(explain, '^[\\s│├└─]+', '')) AS step
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

-- Fractional outer LIMIT (e.g. `LIMIT 0.1`): pushdown must be disabled.
-- A fractional `LIMIT` is evaluated by `FractionalLimitStep`, which must count
-- the full input before deciding how many rows to keep. Pushing it into the
-- view would make the view return only a fraction of its rows, and the outer
-- fractional `LIMIT` would then apply the same fraction again, yielding far
-- too few rows. The correctness check below relies on this: the two-shard
-- cluster reads the 100-row local table on both shards (200 rows), so
-- `LIMIT 0.1` must return 20 rows; buggy pushdown would return far fewer.
SELECT 'fractional LIMIT disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 0.1)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'fractional LIMIT result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 0.1
);

-- `extremes` setting: pushdown must be disabled. The outer planner adds an
-- `ExtremesStep` before the final `LIMIT`, so with `extremes = 1` the extremes
-- are computed over the full pre-`LIMIT` stream. Pushing `LIMIT` into the view
-- would truncate the stream first, so the outer `ExtremesStep` would only see
-- the top-N rows and could report wrong min/max values.
SELECT 'extremes disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10 SETTINGS extremes = 1)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- `exact_rows_before_limit` setting: pushdown must be disabled. The setting
-- promises an exact `rows_before_limit_at_least` value computed over the full
-- pre-`LIMIT` stream. The pushed inner `LIMIT` becomes a child `LimitTransform`
-- under the outer `LimitTransform`, and `initRowsBeforeLimit` ignores child
-- limits once it finds the outer limit, so with the pushdown the counter would
-- be attached above the already-truncated view output and report only the
-- per-shard top-N instead of the full row count.
SELECT 'exact_rows_before_limit disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10 SETTINGS exact_rows_before_limit = 1)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- With the pushdown disabled, `rows_before_limit_at_least` reports the full
-- pre-`LIMIT` row count (200 = 100 rows x 2 shards), not the per-shard top-N.
-- `output_format_write_statistics = 0` keeps the JSON output deterministic.
SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10
FORMAT JSONCompact SETTINGS exact_rows_before_limit = 1, output_format_write_statistics = 0, output_format_json_quote_64bit_integers = 0, max_block_size = 1;

-- View whose declared column type differs from the inner expression type:
-- pushdown must be disabled. `StorageView` converts the inner result to the
-- view's declared structure *after* the inner query runs, so the conversion
-- (here `UInt64` -> `String`) is not order-preserving. The view declares
-- `x String` over a numeric `id`, and the data is `1..12`, where the
-- lexicographic order diverges from the numeric order at the top: the correct
-- (`String`) `ORDER BY x DESC LIMIT 1` is `'9'`, while a buggy numeric pushdown
-- would sort by `UInt64` and return `'12'`. The optimization must skip this
-- view (no merge sort) and the result must be the lexicographic top `'9'`.
DROP TABLE IF EXISTS test_local_strtype_04241;
DROP TABLE IF EXISTS test_distributed_strtype_04241;
DROP VIEW IF EXISTS test_view_strtype_04241;

CREATE TABLE test_local_strtype_04241 (id UInt64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE test_distributed_strtype_04241 AS test_local_strtype_04241
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), test_local_strtype_04241, id);
INSERT INTO test_local_strtype_04241 SELECT number + 1 FROM numbers(12);

CREATE VIEW test_view_strtype_04241 (x String) AS SELECT id AS x FROM test_distributed_strtype_04241;

SELECT 'type mismatch disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT x FROM test_view_strtype_04241 ORDER BY x DESC LIMIT 1)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'type mismatch result:', x FROM test_view_strtype_04241 ORDER BY x DESC LIMIT 1;

DROP VIEW test_view_strtype_04241;
DROP TABLE test_distributed_strtype_04241;
DROP TABLE test_local_strtype_04241;

-- `arrayJoin` in the outer projection: pushdown must be disabled. `arrayJoin`
-- expands each source row into one row per array element and drops rows whose
-- array is empty, so it changes row cardinality *after* the source read. Unlike
-- an `ARRAY JOIN` clause (which the analyzer lowers to a separate table
-- expression, so the outer query is no longer a single-table read and never
-- reaches the optimization), `arrayJoin` in the select list keeps the query
-- single-table and would otherwise pass the guards. The data below gives the
-- highest-`ts` source rows empty arrays, so a buggy pushdown (`ORDER BY ts DESC
-- LIMIT 10` injected into the view) would truncate to those empty-array rows and
-- return 0 expanded rows, while the correct behavior continues to lower-`ts`
-- rows and returns 10.
DROP TABLE IF EXISTS test_local_arrjoin_04241;
DROP TABLE IF EXISTS test_distributed_arrjoin_04241;
DROP VIEW IF EXISTS test_view_arrjoin_04241;

CREATE TABLE test_local_arrjoin_04241 (id UInt64, ts UInt64, arr Array(UInt64))
    ENGINE = MergeTree() ORDER BY id;
-- Rows with the highest `ts` (id >= 90) have empty arrays; lower rows have one element.
INSERT INTO test_local_arrjoin_04241
    SELECT number, number AS ts, if(number >= 90, [], [number]) AS arr FROM numbers(100);

CREATE TABLE test_distributed_arrjoin_04241 AS test_local_arrjoin_04241
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), test_local_arrjoin_04241, id);

CREATE VIEW test_view_arrjoin_04241 AS SELECT id, ts, arr FROM test_distributed_arrjoin_04241;

SELECT 'arrayJoin in projection disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT arrayJoin(arr) AS x FROM test_view_arrjoin_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- Correctness: the top source rows by `ts` all have empty arrays, so the result
-- must come from lower-`ts` rows. With a buggy pushdown this would return 0 rows.
SELECT 'arrayJoin in projection result count:', count() FROM (
    SELECT arrayJoin(arr) AS x FROM test_view_arrjoin_04241 ORDER BY ts DESC LIMIT 10
);

DROP VIEW test_view_arrjoin_04241;
DROP TABLE test_distributed_arrjoin_04241;
DROP TABLE test_local_arrjoin_04241;

-- View whose inner query has a `QUALIFY` clause: pushdown must be disabled.
-- `QUALIFY` filters rows by a window function evaluated over the whole row set;
-- pushing `ORDER BY/LIMIT` into the view would let each shard evaluate the
-- window over only its truncated rows, which is not equivalent to the global
-- computation. The window guards do not catch this because the window lives in
-- `QUALIFY` rather than the select list.
DROP VIEW IF EXISTS test_view_qualify_04241;
CREATE VIEW test_view_qualify_04241 AS
SELECT id, val, ts FROM test_distributed_04241 QUALIFY row_number() OVER (ORDER BY id) > 5;

SELECT 'view QUALIFY disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_qualify_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

DROP VIEW test_view_qualify_04241;

-- View whose inner query carries `LIMIT` through its own `SETTINGS` clause:
-- pushdown must be disabled. `SETTINGS limit = N` constrains which rows the view
-- exposes just like an explicit `LIMIT`, so re-sorting and truncating around it
-- would change which rows the view returns.
DROP VIEW IF EXISTS test_view_settings_limit_04241;
CREATE VIEW test_view_settings_limit_04241 AS
SELECT id, val, ts FROM test_distributed_04241 SETTINGS limit = 5;

SELECT 'view SETTINGS limit disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_settings_limit_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

DROP VIEW test_view_settings_limit_04241;

-- `prefer_column_name_to_alias = 1`: pushdown must be disabled. The injected
-- inner `ORDER BY` references view columns by bare identifier; with this setting
-- an identifier prefers a source column over a matching select-list alias, so
-- the inner sort key could bind to a different expression than the outer view
-- column. Skip the optimization whenever the setting is enabled.
SELECT 'prefer_column_name_to_alias disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10 SETTINGS prefer_column_name_to_alias = 1)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- The same setting carried through the view's own `SETTINGS` clause: the
-- injected inner `ORDER BY` identifiers are resolved under that clause, so it
-- must disable the pushdown even when the outer query keeps the default.
DROP VIEW IF EXISTS test_view_settings_alias_04241;
CREATE VIEW test_view_settings_alias_04241 AS
SELECT id, val, ts FROM test_distributed_04241 SETTINGS prefer_column_name_to_alias = 1;

SELECT 'view SETTINGS prefer_column_name_to_alias disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_settings_alias_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

DROP VIEW test_view_settings_alias_04241;

-- `SQL SECURITY DEFINER` view whose definer settings profile sets `limit`,
-- `offset`, or `prefer_column_name_to_alias`: pushdown must be disabled even
-- though the view's own `SETTINGS` clause is empty. The injected inner
-- `ORDER BY`/`LIMIT` is analyzed and executed under the definer's effective
-- context (`getSQLSecurityOverriddenContext`), so a definer `limit`/`offset`
-- constrains which rows the view exposes (like an inner `LIMIT`/`OFFSET`), and a
-- definer `prefer_column_name_to_alias` rebinds the injected `ORDER BY`
-- identifiers. The AST `SETTINGS` guard above does not see these inherited
-- settings, so they must be rejected via the effective view context.
-- Drop the dependent views before the users so this block is rerunnable even if
-- a previous run was interrupted between creating a view and dropping its user.
DROP VIEW IF EXISTS test_view_definer_limit_04241;
DROP VIEW IF EXISTS test_view_definer_offset_04241;
DROP VIEW IF EXISTS test_view_definer_alias_04241;
DROP USER IF EXISTS definer_limit_04241;
DROP USER IF EXISTS definer_offset_04241;
DROP USER IF EXISTS definer_alias_04241;
CREATE USER definer_limit_04241 IDENTIFIED WITH no_password SETTINGS limit = 5;
CREATE USER definer_offset_04241 IDENTIFIED WITH no_password SETTINGS offset = 5;
CREATE USER definer_alias_04241 IDENTIFIED WITH no_password SETTINGS prefer_column_name_to_alias = 1;
GRANT SELECT ON *.* TO definer_limit_04241, definer_offset_04241, definer_alias_04241;

DROP VIEW IF EXISTS test_view_definer_limit_04241;
CREATE VIEW test_view_definer_limit_04241
    DEFINER = definer_limit_04241 SQL SECURITY DEFINER
    AS SELECT id, val, ts FROM test_distributed_04241;
SELECT 'definer profile limit disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_definer_limit_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;
DROP VIEW test_view_definer_limit_04241;

DROP VIEW IF EXISTS test_view_definer_offset_04241;
CREATE VIEW test_view_definer_offset_04241
    DEFINER = definer_offset_04241 SQL SECURITY DEFINER
    AS SELECT id, val, ts FROM test_distributed_04241;
SELECT 'definer profile offset disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_definer_offset_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;
DROP VIEW test_view_definer_offset_04241;

DROP VIEW IF EXISTS test_view_definer_alias_04241;
CREATE VIEW test_view_definer_alias_04241
    DEFINER = definer_alias_04241 SQL SECURITY DEFINER
    AS SELECT id, val, ts FROM test_distributed_04241;
SELECT 'definer profile prefer_column_name_to_alias disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_definer_alias_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;
DROP VIEW test_view_definer_alias_04241;

DROP USER definer_limit_04241;
DROP USER definer_offset_04241;
DROP USER definer_alias_04241;

-- `FINAL` applied to the view in the outer query: pushdown must be disabled.
-- `FINAL` selects which rows the view exposes (collapsing duplicates in the
-- underlying MergeTree family), but the pushed-down inner `ORDER BY`/`LIMIT` is
-- rebuilt from the view's stored definition, which carries no `FINAL`, so it
-- would order and truncate the pre-collapse row set below the point where
-- `FINAL` applies and could return the wrong top-N.
SELECT 'FINAL disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 FINAL ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'FINAL result count:', count() FROM (
    SELECT id FROM test_view_04241 FINAL ORDER BY ts DESC LIMIT 10
);

-- `SAMPLE` applied to the view in the outer query: pushdown must be disabled for
-- the same reason — `SAMPLE` restricts the view to a pseudo-random subset, so
-- pushing `ORDER BY`/`LIMIT` into the unsampled inner query would order and
-- truncate the full row set instead of the sampled subset.
DROP TABLE IF EXISTS test_local_sample_04241;
DROP TABLE IF EXISTS test_distributed_sample_04241;
DROP VIEW IF EXISTS test_view_sample_04241;

CREATE TABLE test_local_sample_04241 (id UInt64, ts UInt64)
    ENGINE = MergeTree() ORDER BY id SAMPLE BY id;
INSERT INTO test_local_sample_04241 SELECT number, number FROM numbers(100);

CREATE TABLE test_distributed_sample_04241 AS test_local_sample_04241
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), test_local_sample_04241, id);

CREATE VIEW test_view_sample_04241 AS SELECT id, ts FROM test_distributed_sample_04241;

SELECT 'SAMPLE disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_sample_04241 SAMPLE 0.1 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

DROP VIEW test_view_sample_04241;
DROP TABLE test_distributed_sample_04241;
DROP TABLE test_local_sample_04241;

DROP TABLE test_join_04241;
DROP VIEW test_view_04241;
DROP TABLE test_distributed_04241;
DROP TABLE test_local_04241;
