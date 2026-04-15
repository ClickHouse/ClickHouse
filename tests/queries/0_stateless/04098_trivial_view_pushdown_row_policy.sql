-- Row policies at the view and distributed table levels must be enforced
-- when optimize_trivial_view_pushdown_to_distributed fires.
-- id=2: hidden by view policy, id=3: by dist policy.
-- id=1, id=4 and id=5 must always be visible.
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
DROP ROW POLICY IF EXISTS 04098_policy_dist  ON 04098_dist;

CREATE TABLE 04098_local (id UInt32, val Int32)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04098_dist AS 04098_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04098_local);

CREATE VIEW 04098_view AS SELECT id, val FROM 04098_dist;

-- Aliased view: the view produces a computed alias `v` (= val * 2).
CREATE VIEW 04098_view_alias AS SELECT id, val * 2 AS v FROM 04098_dist;

INSERT INTO 04098_dist VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500);
SYSTEM FLUSH DISTRIBUTED 04098_dist;

-- Baseline: no policies, all 5 rows visible.
SELECT count() FROM 04098_view;

CREATE ROW POLICY 04098_policy_view  ON 04098_view  USING id != 2 TO ALL;
CREATE ROW POLICY 04098_policy_dist  ON 04098_dist  USING id != 3 TO ALL;

-- View and dist policies enforced: id=1, id=4 and id=5 survive.
SET optimize_trivial_view_pushdown_to_distributed = 1;
SELECT id, val FROM 04098_view ORDER BY id;

-- Policy references the computed alias `v` directly; the analyzer must expand it to `val * 2`.
-- This hides id=2 (val=200 → v=400) just as `id != 2` would, keeping the reference output identical.
CREATE ROW POLICY 04098_policy_view_alias ON 04098_view_alias USING v != 400 TO ALL;
SELECT id, v FROM 04098_view_alias ORDER BY id;
DROP ROW POLICY 04098_policy_view_alias ON 04098_view_alias;

-- View and dist policies must appear in system.query_log.used_row_policies.
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
LIMIT 1;

DROP ROW POLICY 04098_policy_view  ON 04098_view;
DROP ROW POLICY 04098_policy_dist  ON 04098_dist;
DROP VIEW 04098_view_alias;
DROP VIEW 04098_view;
DROP TABLE 04098_dist;
DROP TABLE 04098_local;
