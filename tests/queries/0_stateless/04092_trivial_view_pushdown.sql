-- Tests for modifier merging (FINAL, SAMPLE/OFFSET) in trivial-view pushdown
-- to Distributed tables (optimize_trivial_view_pushdown_to_distributed).
-- Tags: distributed

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_trivial_view_pushdown_to_distributed = 1;
-- TCP path: exercises real distributed execution (in-process shortcut is a no-op).
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04092_local_replacing;
DROP TABLE IF EXISTS 04092_dist_replacing;
DROP VIEW IF EXISTS 04092_view_replacing;
DROP TABLE IF EXISTS 04092_local_sampled;
DROP TABLE IF EXISTS 04092_dist_sampled;
DROP VIEW IF EXISTS 04092_view_sampled;

-- -----------------------------------------------------------------------
-- Test 1: FINAL is propagated to the shard (ReplacingMergeTree deduplication).
-- -----------------------------------------------------------------------
CREATE TABLE 04092_local_replacing (id UInt32, val UInt32, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY id;

CREATE TABLE 04092_dist_replacing AS 04092_local_replacing
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04092_local_replacing);

CREATE VIEW 04092_view_replacing AS SELECT * FROM 04092_dist_replacing;

SYSTEM STOP MERGES 04092_local_replacing;

INSERT INTO 04092_dist_replacing VALUES (1, 10, 1);
INSERT INTO 04092_dist_replacing VALUES (1, 20, 2);
INSERT INTO 04092_dist_replacing VALUES (2, 30, 1);
SYSTEM FLUSH DISTRIBUTED 04092_dist_replacing;

-- Without FINAL both versions of id=1 are visible.
SELECT count() FROM 04092_view_replacing;

-- With FINAL only the latest version survives.
SELECT id, val FROM 04092_view_replacing FINAL ORDER BY id;

SYSTEM START MERGES 04092_local_replacing;

-- -----------------------------------------------------------------------
-- Test 2: SAMPLE and OFFSET are propagated to the shard.
-- -----------------------------------------------------------------------
CREATE TABLE 04092_local_sampled (id UInt64)
ENGINE = MergeTree ORDER BY intHash64(id) SAMPLE BY intHash64(id);

CREATE TABLE 04092_dist_sampled AS 04092_local_sampled
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04092_local_sampled);

CREATE VIEW 04092_view_sampled AS SELECT * FROM 04092_dist_sampled;

INSERT INTO 04092_dist_sampled SELECT number FROM numbers(1000);
SYSTEM FLUSH DISTRIBUTED 04092_dist_sampled;

-- SAMPLE 1 returns all rows.
SELECT count() FROM 04092_view_sampled SAMPLE 1;

-- Complementary halves must be disjoint.
SELECT count() FROM (
    SELECT id FROM 04092_view_sampled SAMPLE 1/2 OFFSET 0
    INTERSECT
    SELECT id FROM 04092_view_sampled SAMPLE 1/2 OFFSET 1/2
);

-- -----------------------------------------------------------------------
-- Test 3: Query plan confirms the optimization fires.
-- prefer_localhost_replica = 1 here as coverage for that execution path.
-- -----------------------------------------------------------------------
SET prefer_localhost_replica = 1;

CREATE VIEW 04092_view_expr AS
    SELECT id, val + 1 AS adjusted_val FROM 04092_dist_replacing WHERE id != 0;

-- Optimization ON: no "VIEW subquery" steps in the plan.
SET optimize_trivial_view_pushdown_to_distributed = 1;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS optimization_fired
FROM (EXPLAIN SELECT * FROM 04092_view_expr FINAL);

-- Optimization OFF: "VIEW subquery" steps are present.
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS view_subquery_present
FROM (EXPLAIN SELECT * FROM 04092_view_expr FINAL);

SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP VIEW 04092_view_expr;

-- -----------------------------------------------------------------------
-- Test 4: Non-deterministic / server-local functions in the OUTER query
-- suppress the optimization. The view body is read through
-- StorageDistributed::read either way, so body non-determinism would not
-- change semantics; the hazard is outer expressions, which the pushdown
-- would otherwise ship to shards and evaluate per-shard (`hostName`,
-- `serverUUID`, `nowInBlock`, `blockNumber`, `rand`, `now`, ...).
-- -----------------------------------------------------------------------
SET prefer_localhost_replica = 0;

-- hostName() in outer projection.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT hostName() FROM 04092_view_replacing);

-- rand() in outer projection.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT rand() FROM 04092_view_replacing);

-- rand() in outer WHERE.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04092_view_replacing WHERE rand() < 1);

-- -----------------------------------------------------------------------
-- Test 5: Column transformers on asterisks suppress the optimization.
-- APPLY/REPLACE/EXCEPT on * or t.* can carry aggregate, window, or
-- non-deterministic expressions and must not be treated as trivial.
-- -----------------------------------------------------------------------

-- SELECT * APPLY(rand()) suppresses pushdown.
CREATE VIEW 04092_view_asterisk_apply AS
    SELECT * APPLY(rand()) FROM 04092_dist_replacing;

SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT * FROM 04092_view_asterisk_apply);

DROP VIEW 04092_view_asterisk_apply;

-- SELECT * EXCEPT(val) suppresses pushdown.
CREATE VIEW 04092_view_asterisk_except AS
    SELECT * EXCEPT(val) FROM 04092_dist_replacing;

SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT * FROM 04092_view_asterisk_except);

DROP VIEW 04092_view_asterisk_except;

-- -----------------------------------------------------------------------
-- Test 6: additional_table_filters are honored under the pushdown, whether
-- keyed by the view name or by the underlying Distributed table name,
-- matching the non-pushdown path.
--   * View-keyed filters are folded into the outer query's WHERE before the
--     view body is inlined, so they ride along inside the shard query (and
--     resolve against the view's namespace, including renamed columns).
--   * Distributed-table-keyed filters are parsed for the underlying table so
--     StorageDistributed propagates them to the shard-local table.
-- 04092_dist_replacing carries 3 rows with id values {1, 1, 2}, so a filter
-- "id != 2" / "aid != 2" must keep 2 of them.
-- -----------------------------------------------------------------------
SET optimize_trivial_view_pushdown_to_distributed = 1;
SET prefer_localhost_replica = 0;

-- View-keyed, unaliased view: filter references the view's own column name.
SELECT count() FROM 04092_view_replacing
SETTINGS additional_table_filters = {'04092_view_replacing': 'id != 2'};

-- View-keyed, aliased view: filter references the view's alias, which has no
-- counterpart on the shard-local table.
CREATE VIEW 04092_view_aliased AS SELECT id AS aid FROM 04092_dist_replacing;

SELECT count() FROM 04092_view_aliased
SETTINGS additional_table_filters = {'04092_view_aliased': 'aid != 2'};

DROP VIEW 04092_view_aliased;

-- Distributed-table-keyed: filter references the underlying dist table by name.
-- The non-pushdown path applies it (propagated to the shard); the pushdown must too.
SELECT count() FROM 04092_view_replacing
SETTINGS additional_table_filters = {'04092_dist_replacing': 'id != 2'};

DROP VIEW 04092_view_replacing;
DROP TABLE 04092_dist_replacing;
DROP TABLE 04092_local_replacing;
DROP VIEW 04092_view_sampled;
DROP TABLE 04092_dist_sampled;
DROP TABLE 04092_local_sampled;
