-- A row policy on the view (or on the underlying Distributed table) suppresses the
-- trivial-view pushdown. Row policies must be enforced in the view-output namespace,
-- which the canonical StorageView::readImpl path does correctly; the pushdown would
-- inline the body and could bind the policy to a source column instead of the view
-- alias (e.g. `SELECT val * 2 AS v` with policy on `v` under
-- prefer_column_name_to_alias = 1), so whenever a policy applies we fall back to
-- readImpl. This test asserts both that the pushdown is suppressed (plan keeps the
-- "VIEW subquery" steps) and that the result is correct, identical with the setting
-- on and off.
--
-- Tags: distributed

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04098_local;
DROP TABLE IF EXISTS 04098_dist;
DROP VIEW IF EXISTS 04098_view;
DROP VIEW IF EXISTS 04098_view_alias;
DROP ROW POLICY IF EXISTS 04098_policy_view  ON 04098_view;

CREATE TABLE 04098_local (id UInt32, val Int32)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04098_dist AS 04098_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04098_local);

CREATE VIEW 04098_view AS SELECT id, val FROM 04098_dist;

-- Aliased view: the view produces a computed alias `v` (= val * 2).
CREATE VIEW 04098_view_alias AS SELECT id, val * 2 AS v FROM 04098_dist;

INSERT INTO 04098_dist VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500);
SYSTEM FLUSH DISTRIBUTED 04098_dist;

SET optimize_trivial_view_pushdown_to_distributed = 1;

-- Baseline: no policy, all 5 rows visible and the pushdown fires (no "VIEW subquery").
SELECT count() FROM 04098_view;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT id, val FROM 04098_view);

CREATE ROW POLICY 04098_policy_view  ON 04098_view  USING id != 2 TO ALL;

-- With a view policy the pushdown is suppressed: plan keeps the "VIEW subquery" steps.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id, val FROM 04098_view);

-- The view policy is enforced (id=2 hidden); result is identical with the setting off.
SELECT id, val FROM 04098_view ORDER BY id;
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id, val FROM 04098_view ORDER BY id;
SET optimize_trivial_view_pushdown_to_distributed = 1;

-- Policy references the computed alias `v` directly; the pushdown is suppressed and
-- readImpl expands it to `val * 2`, hiding id=2 (val=200 → v=400).
CREATE ROW POLICY 04098_policy_view_alias ON 04098_view_alias USING v != 400 TO ALL;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id, v FROM 04098_view_alias);
SELECT id, v FROM 04098_view_alias ORDER BY id;
DROP ROW POLICY 04098_policy_view_alias ON 04098_view_alias;

-- A row policy on the underlying Distributed table also suppresses the pushdown.
CREATE ROW POLICY 04098_policy_dist ON 04098_dist USING id != 3 TO ALL;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id, val FROM 04098_view);
DROP ROW POLICY 04098_policy_dist ON 04098_dist;

-- The view policy must appear in system.query_log.used_row_policies.
SYSTEM FLUSH LOGS query_log;
SELECT arraySort(arrayFilter(p -> p NOT LIKE '%04098_local%', used_row_policies))
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%SELECT id, val FROM 04098_view ORDER BY id%'
    AND query NOT LIKE '%system.query_log%'
    AND query NOT LIKE '%EXPLAIN%'
LIMIT 1;

-- View in a join: the pushdown is suppressed (it only fires for a sole table
-- expression), so the view falls back to the standard path. The view policy
-- (id != 2) must still be enforced — the joined local row id=2 finds no matching
-- view row and is null-extended (val = 0). Result must not depend on the setting.
CREATE TABLE 04098_join_local (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO 04098_join_local VALUES (1), (2), (3);

SELECT 04098_join_local.id, 04098_view.val
FROM 04098_join_local LEFT JOIN 04098_view ON 04098_join_local.id = 04098_view.id
ORDER BY 04098_join_local.id;

DROP TABLE 04098_join_local;

DROP ROW POLICY 04098_policy_view  ON 04098_view;
DROP VIEW 04098_view_alias;
DROP VIEW 04098_view;
DROP TABLE 04098_dist;
DROP TABLE 04098_local;
