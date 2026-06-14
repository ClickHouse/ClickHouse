-- Subqueries in the pushed-down portion of a trivial-view-over-Distributed query suppress
-- optimize_trivial_view_pushdown_to_distributed. A subquery (e.g. the right-hand side of
-- `IN (SELECT ...)`) is evaluated once on the coordinator on the normal StorageView path, but
-- per-shard once the pushdown ships the outer query to the shards. If it reads an initiator-local
-- table, or one whose contents/privileges differ between shards, the result can change or the
-- query can start throwing. So the optimization must bail out. The three places the pushdown
-- moves a predicate onto the shards are exercised here: the outer query, a view-keyed
-- additional_table_filter, and the view's row policy.
--
-- Tags: distributed

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_trivial_view_pushdown_to_distributed = 1;
-- TCP path: exercises real distributed execution (in-process shortcut is a no-op).
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04338_local;
DROP TABLE IF EXISTS 04338_dist;
DROP TABLE IF EXISTS 04338_filter;
DROP VIEW IF EXISTS 04338_view;
DROP ROW POLICY IF EXISTS 04338_policy_subquery ON 04338_view;

CREATE TABLE 04338_local (id UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04338_dist AS 04338_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04338_local);

CREATE VIEW 04338_view AS SELECT * FROM 04338_dist;

-- Initiator-local table referenced by the subqueries.
CREATE TABLE 04338_filter (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO 04338_dist VALUES (1), (2), (3);
SYSTEM FLUSH DISTRIBUTED 04338_dist;
INSERT INTO 04338_filter VALUES (1), (2);

-- Positive control: a plain query (no subquery) still fires the pushdown, so the gate is
-- specific to subqueries and does not suppress everything.
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS optimization_fired
FROM (EXPLAIN SELECT id FROM 04338_view WHERE id != 0);

-- IN (subquery) in the outer WHERE suppresses the pushdown.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04338_view WHERE id IN (SELECT id FROM 04338_filter));

-- NOT IN (subquery) in the outer WHERE suppresses the pushdown.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04338_view WHERE id NOT IN (SELECT id FROM 04338_filter));

-- A subquery in a view-keyed additional_table_filter suppresses the pushdown: the filter is
-- folded into the outer WHERE and would ride along inside the shipped query.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04338_view
      SETTINGS additional_table_filters = {'04338_view': 'id IN (SELECT id FROM 04338_filter)'});

-- A subquery in the view's row policy suppresses the pushdown: the policy is injected into the
-- inner WHERE and would otherwise be evaluated per-shard.
CREATE ROW POLICY 04338_policy_subquery ON 04338_view USING id IN (SELECT id FROM 04338_filter) TO ALL;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04338_view);
DROP ROW POLICY 04338_policy_subquery ON 04338_view;

-- Correctness: the suppressed queries still return the right rows via the fallback path.
-- 04338_dist holds {1, 2, 3}; 04338_filter holds {1, 2}.
SELECT id FROM 04338_view WHERE id IN (SELECT id FROM 04338_filter) ORDER BY id;
SELECT id FROM 04338_view WHERE id NOT IN (SELECT id FROM 04338_filter) ORDER BY id;

DROP VIEW 04338_view;
DROP TABLE 04338_dist;
DROP TABLE 04338_local;
DROP TABLE 04338_filter;
