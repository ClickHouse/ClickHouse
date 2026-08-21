-- A SETTINGS clause in a trivial view's body suppresses
-- optimize_trivial_view_pushdown_to_distributed. Some query-level settings (notably `limit` and
-- `offset`) are turned into QueryNode limit/offset by QueryTreeBuilder, so they are not visible as
-- explicit LIMIT/OFFSET clauses but still change the result. A body such as
-- `SELECT id FROM dist SETTINGS limit = 1` is limited once globally on the normal StorageView path,
-- but once per shard if the outer query is pushed down. `SELECT count() FROM v` would then return
-- the per-shard count summed across shards instead of 1. Rather than enumerate every result-changing
-- setting, the trivial-view check fails close on any SETTINGS clause in the body.
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
DROP VIEW IF EXISTS 04357_view_limit;
DROP VIEW IF EXISTS 04357_view_offset;

CREATE TABLE 04357_local (id UInt32) ENGINE = MergeTree ORDER BY id;

-- Two shards, both reading the same local table: querying the Distributed table returns each row
-- twice, so a per-shard limit/offset differs observably from a global one.
CREATE TABLE 04357_dist AS 04357_local
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 04357_local);

INSERT INTO 04357_local VALUES (1), (2), (3);

-- Positive control: a settings-free trivial view still fires the pushdown, so the gate is specific
-- to the SETTINGS clause and does not suppress everything.
CREATE VIEW 04357_view_plain AS SELECT id FROM 04357_dist;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS optimization_fired
FROM (EXPLAIN SELECT count() FROM 04357_view_plain);

-- `SETTINGS limit = ...` in the body suppresses the pushdown.
CREATE VIEW 04357_view_limit AS SELECT id FROM 04357_dist SETTINGS limit = 1;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT count() FROM 04357_view_limit);

-- `SETTINGS offset = ...` in the body suppresses the pushdown.
CREATE VIEW 04357_view_offset AS SELECT id FROM 04357_dist SETTINGS offset = 1;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT count() FROM 04357_view_offset);

-- Correctness: the body limit is applied once globally over the 6 rows (3 per shard), so the count
-- is 1. With the bypass it would have been 2 (one per shard).
SELECT count() FROM 04357_view_limit;

DROP VIEW 04357_view_plain;
DROP VIEW 04357_view_limit;
DROP VIEW 04357_view_offset;
DROP TABLE 04357_dist;
DROP TABLE 04357_local;
