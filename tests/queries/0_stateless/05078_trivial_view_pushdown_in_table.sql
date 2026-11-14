-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/116839
-- `optimize_trivial_view_pushdown_to_distributed` ships the outer query of a trivial view over a
-- `Distributed` table to the shards, and suppresses itself when the outer query contains a subquery,
-- because a subquery reading an initiator-local table changes its result when re-evaluated per shard.
-- A bare `IN <table>` carries a TABLE node rather than a QUERY node and slipped through, so the two
-- spellings of the same predicate returned different rows. The same held for a view-keyed
-- `additional_table_filters` predicate, which is screened at the AST level, where a bare `IN <table>`
-- is an identifier rather than an `ASTSubquery`.
--
-- A single-server rig cannot observe the result divergence itself: the shipped predicate is
-- database-qualified with the initiator's database, so every "shard" resolves it to the same physical
-- table. The plan-shape assertions below (the "VIEW subquery" steps survive only when the pushdown is
-- suppressed) are what pins the fix; the result checks guard the fallback path.

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;
SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP TABLE IF EXISTS t_view_pushdown_data;
DROP TABLE IF EXISTS t_view_pushdown_probe;
DROP TABLE IF EXISTS d_view_pushdown;
DROP VIEW IF EXISTS v_view_pushdown;

CREATE TABLE t_view_pushdown_data (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_view_pushdown_probe (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_view_pushdown_data SELECT number FROM numbers(10);
INSERT INTO t_view_pushdown_probe VALUES (1), (2);

CREATE TABLE d_view_pushdown AS t_view_pushdown_data
ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_view_pushdown_data);
CREATE VIEW v_view_pushdown AS SELECT * FROM d_view_pushdown;

-- Positive control: a predicate with no table reference still fires the pushdown.
SELECT 'outer WHERE';
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS optimization_fired
FROM (EXPLAIN SELECT id FROM v_view_pushdown WHERE id > 5);
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS optimization_fired
FROM (EXPLAIN SELECT id FROM v_view_pushdown WHERE id IN (1, 2));

-- A bare `IN <table>` / `NOT IN <table>` / `GLOBAL IN <table>` suppresses the pushdown, like `IN (SELECT ...)` does.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM v_view_pushdown WHERE id IN t_view_pushdown_probe);
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM v_view_pushdown WHERE id NOT IN t_view_pushdown_probe);
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM v_view_pushdown WHERE id GLOBAL IN t_view_pushdown_probe);

-- The two spellings of the same predicate must agree, and neither may depend on the setting.
SELECT id FROM v_view_pushdown WHERE id IN t_view_pushdown_probe ORDER BY id;
SELECT id FROM v_view_pushdown WHERE id IN t_view_pushdown_probe ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM v_view_pushdown WHERE id IN (SELECT id FROM t_view_pushdown_probe) ORDER BY id;

SELECT 'not in';
SELECT count() FROM v_view_pushdown WHERE id NOT IN t_view_pushdown_probe;
SELECT count() FROM v_view_pushdown WHERE id NOT IN t_view_pushdown_probe SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT count() FROM v_view_pushdown WHERE id NOT IN (SELECT id FROM t_view_pushdown_probe);

SELECT 'still pushed down';
SELECT count() FROM v_view_pushdown WHERE id > 5;
SELECT count() FROM v_view_pushdown WHERE id IN (1, 2);

-- View-keyed additional_table_filters are folded into the shipped WHERE; a bare `IN <table>` there
-- must suppress the pushdown just like a subquery does (see 04509).
SELECT 'additional_table_filters';
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS optimization_fired
FROM (EXPLAIN SELECT id FROM v_view_pushdown
      SETTINGS additional_table_filters = {'v_view_pushdown': 'id > 5'});
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM v_view_pushdown
      SETTINGS additional_table_filters = {'v_view_pushdown': 'id IN t_view_pushdown_probe'});
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM v_view_pushdown
      SETTINGS additional_table_filters = {'v_view_pushdown': 'id NOT IN t_view_pushdown_probe'});
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM v_view_pushdown
      SETTINGS additional_table_filters = {'v_view_pushdown': 'id GLOBAL IN t_view_pushdown_probe'});

SELECT id FROM v_view_pushdown ORDER BY id
SETTINGS additional_table_filters = {'v_view_pushdown': 'id IN t_view_pushdown_probe'};
SELECT id FROM v_view_pushdown ORDER BY id
SETTINGS additional_table_filters = {'v_view_pushdown': 'id IN t_view_pushdown_probe'}, optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM v_view_pushdown ORDER BY id
SETTINGS additional_table_filters = {'v_view_pushdown': 'id IN (SELECT id FROM t_view_pushdown_probe)'};
SELECT count() FROM v_view_pushdown
SETTINGS additional_table_filters = {'v_view_pushdown': 'id NOT IN t_view_pushdown_probe'};
SELECT count() FROM v_view_pushdown
SETTINGS additional_table_filters = {'v_view_pushdown': 'id NOT IN t_view_pushdown_probe'}, optimize_trivial_view_pushdown_to_distributed = 0;

DROP VIEW v_view_pushdown;
DROP TABLE d_view_pushdown;
DROP TABLE t_view_pushdown_probe;
DROP TABLE t_view_pushdown_data;
