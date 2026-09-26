-- A SETTINGS clause in a trivial view's body suppresses
-- optimize_trivial_view_pushdown_to_distributed. Some query-level settings are turned into plan
-- properties rather than explicit clauses, so they are not visible as `LIMIT`/`OFFSET`/`WHERE` in
-- the body but still change the result: applied once globally on the normal `StorageView` path, but
-- once per shard if the outer query is pushed down. Rather than enumerate every result-changing
-- setting, `tryGetTrivialViewUnderlyingStorage` fails close on any SETTINGS clause in the body, so
-- the checks below only need some setting present — the specific name does not matter.
--
-- `limit` / `offset` were the original motivating examples here, but they are query-construction
-- settings and are rejected outright in a `VIEW` definition (see
-- `InterpreterCreateQuery::createTable`), so a view carrying them cannot be created any more; that
-- rejection is covered by `04367_construction_settings_in_view.sql`.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET optimize_trivial_view_pushdown_to_distributed = 1;
-- TCP path: exercises real distributed execution (in-process shortcut is a no-op).
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04357_local;
DROP TABLE IF EXISTS 04357_dist;
DROP VIEW IF EXISTS 04357_view_plain;
DROP VIEW IF EXISTS 04357_view_settings;
DROP VIEW IF EXISTS 04357_view_settings2;

CREATE TABLE 04357_local (id UInt32) ENGINE = MergeTree ORDER BY id;

-- Two shards, both reading the same local table: querying the Distributed table returns each row
-- twice, so a per-shard application of a result-shaping setting differs observably from a global one.
CREATE TABLE 04357_dist AS 04357_local
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 04357_local);

INSERT INTO 04357_local VALUES (1), (2), (3);

-- Positive control: a settings-free trivial view still fires the pushdown, so the gate is specific
-- to the SETTINGS clause and does not suppress everything.
CREATE VIEW 04357_view_plain AS SELECT id FROM 04357_dist;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS optimization_fired
FROM (EXPLAIN SELECT count() FROM 04357_view_plain);

-- A SETTINGS clause in the body suppresses the pushdown.
CREATE VIEW 04357_view_settings AS SELECT id FROM 04357_dist SETTINGS max_block_size = 1;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT count() FROM 04357_view_settings);

-- The gate does not look at which setting it is, so an unrelated one suppresses it just the same.
CREATE VIEW 04357_view_settings2 AS SELECT id FROM 04357_dist SETTINGS optimize_move_to_prewhere = 0;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT count() FROM 04357_view_settings2);

-- Correctness: falling back to the `StorageView` path still returns every row of the two-shard
-- Distributed table (3 rows per shard), so the count is 6.
SELECT count() FROM 04357_view_settings;

DROP VIEW 04357_view_plain;
DROP VIEW 04357_view_settings;
DROP VIEW 04357_view_settings2;
DROP TABLE 04357_dist;
DROP TABLE 04357_local;
