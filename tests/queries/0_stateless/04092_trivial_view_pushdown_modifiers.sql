-- Regression test for modifier merging (FINAL, SAMPLE/OFFSET) in trivial-view pushdown
-- to Distributed tables (optimize_trivial_view_pushdown_to_distributed).
-- Tags: distributed

SET enable_parallel_replicas = 0;
SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP TABLE IF EXISTS 04092_local_replacing;
DROP TABLE IF EXISTS 04092_dist_replacing;
DROP VIEW IF EXISTS 04092_view_replacing;
DROP TABLE IF EXISTS 04092_local_sampled;
DROP TABLE IF EXISTS 04092_dist_sampled;
DROP VIEW IF EXISTS 04092_view_sampled;

-- -----------------------------------------------------------------------
-- Test 1: FINAL is propagated through the trivial-view pushdown path.
-- A ReplacingMergeTree is used so that FINAL deduplicates at the shard level.
-- -----------------------------------------------------------------------
CREATE TABLE 04092_local_replacing (id UInt32, val UInt32, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY id;

CREATE TABLE 04092_dist_replacing AS 04092_local_replacing
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04092_local_replacing);

CREATE VIEW 04092_view_replacing AS SELECT * FROM 04092_dist_replacing;

SYSTEM STOP MERGES 04092_local_replacing;

-- Insert the same key twice in separate batches so two parts exist.
INSERT INTO 04092_local_replacing VALUES (1, 10, 1);
INSERT INTO 04092_local_replacing VALUES (1, 20, 2);
INSERT INTO 04092_local_replacing VALUES (2, 30, 1);

-- Without FINAL both versions of id=1 are visible.
SELECT count() FROM 04092_view_replacing;

-- With FINAL only the latest version survives.
SELECT id, val FROM 04092_view_replacing FINAL ORDER BY id;

SYSTEM START MERGES 04092_local_replacing;

-- -----------------------------------------------------------------------
-- Test 2: SAMPLE and OFFSET from the outer query are propagated.
-- Using SAMPLE 1 (full sample) verifies the modifier reaches the shard.
-- Using complementary SAMPLE 1/2 OFFSET 0 / SAMPLE 1/2 OFFSET 1/2 verifies
-- that the offset value is passed correctly: the two halves must be disjoint.
-- -----------------------------------------------------------------------
CREATE TABLE 04092_local_sampled (id UInt64)
ENGINE = MergeTree ORDER BY intHash64(id) SAMPLE BY intHash64(id);

CREATE TABLE 04092_dist_sampled AS 04092_local_sampled
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04092_local_sampled);

CREATE VIEW 04092_view_sampled AS SELECT * FROM 04092_dist_sampled;

INSERT INTO 04092_local_sampled SELECT number FROM numbers(1000);

-- SAMPLE 1 must return every row.
SELECT count() FROM 04092_view_sampled SAMPLE 1;

-- SAMPLE 1/2 OFFSET 0 and SAMPLE 1/2 OFFSET 1/2 cover complementary hash
-- ranges and must not share any rows.
SELECT count() FROM (
    SELECT id FROM 04092_view_sampled SAMPLE 1/2 OFFSET 0
    INTERSECT
    SELECT id FROM 04092_view_sampled SAMPLE 1/2 OFFSET 1/2
);

-- -----------------------------------------------------------------------
-- Test 3: Verify the optimization is actually applied by inspecting the
-- query plan. StorageView::read injects two ExpressionSteps whose
-- descriptions contain "VIEW subquery" (lines 378 and 402 of StorageView.cpp).
-- When the optimization fires, StorageView::read is bypassed entirely and
-- those steps never appear. When it is disabled, they do appear.
--
-- A view with an expression alias and a simple filter is used here to
-- exercise the non-trivial-but-still-eligible SELECT list path inside
-- tryGetTrivialViewUnderlyingStorage.
-- -----------------------------------------------------------------------
CREATE VIEW 04092_view_expr AS
    SELECT id, val + 1 AS adjusted_val FROM 04092_dist_replacing WHERE id != 0;

-- Optimization ON: no "VIEW subquery" step descriptions in the plan.
SET optimize_trivial_view_pushdown_to_distributed = 1;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS optimization_fired
FROM (EXPLAIN SELECT * FROM 04092_view_expr FINAL);

-- Optimization OFF: "VIEW subquery" step descriptions are present.
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS view_subquery_present
FROM (EXPLAIN SELECT * FROM 04092_view_expr FINAL);

-- Restore.
SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP VIEW 04092_view_expr;
DROP VIEW 04092_view_replacing;
DROP TABLE 04092_dist_replacing;
DROP TABLE 04092_local_replacing;
DROP VIEW 04092_view_sampled;
DROP TABLE 04092_dist_sampled;
DROP TABLE 04092_local_sampled;
