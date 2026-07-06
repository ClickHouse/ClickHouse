-- Regression: EXPLAIN PIPELINE / EXPLAIN ESTIMATE over a query with
-- MATERIALIZED CTEs must not abort the server in debug or sanitizer builds.
-- See PR #105041 AST fuzzer failure - the destructor of
-- `MaterializingCTETransform` constructed `Exception(LOGICAL_ERROR, ...)` on a
-- legitimate non-execution path (pipeline built but never executed), which
-- in debug builds triggers `abortOnFailedAssertion` in the `Exception` ctor.
--
-- The EXPLAIN queries below are wrapped in `SELECT count() > 0 FROM (...)`
-- because `EXPLAIN PIPELINE` emits multi-line text that is not suppressed by
-- a per-query `FORMAT Null` clause; wrapping in a subquery lets us assert
-- that EXPLAIN produced output without printing it.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_04260 SYNC;
CREATE TABLE t_04260 (c Int32) ENGINE = MergeTree ORDER BY c;
INSERT INTO t_04260 VALUES (1), (2), (3);

-- 1. EXPLAIN PIPELINE over a SELECT with two MATERIALIZED CTEs, one of which
--    references the other. This is the exact fuzzer trigger.
SELECT count() > 0 FROM (
  EXPLAIN PIPELINE
  WITH ct AS MATERIALIZED (SELECT DISTINCT c FROM t_04260 LIMIT 2147483648),
       rs AS MATERIALIZED (SELECT 65537, * FROM t_04260 WHERE c IN (SELECT c FROM ct GROUP BY 1))
  SELECT -2 FROM rs AS a, rs AS b
);

-- 2. EXPLAIN PIPELINE graph=1 - the graph variant (path through
--    QueryPipelineBuilder::getPipe + printPipeline).
SELECT count() > 0 FROM (
  EXPLAIN PIPELINE graph = 1
  WITH ct AS MATERIALIZED (SELECT DISTINCT c FROM t_04260)
  SELECT c FROM ct
);

-- 3. EXPLAIN ESTIMATE - the other fuzzer-failing branch.
SELECT count() >= 0 FROM (
  EXPLAIN ESTIMATE
  WITH ct AS MATERIALIZED (SELECT DISTINCT c FROM t_04260)
  SELECT c FROM ct
);

-- 4. EXPLAIN PIPELINE with a CTE referenced in an IN (subquery) - exercises
--    the buildOrderedSetInplace path's safety-net DelayedMaterializingCTEsStep.
SELECT count() > 0 FROM (
  EXPLAIN PIPELINE
  WITH ct AS MATERIALIZED (SELECT c FROM t_04260)
  SELECT c FROM t_04260 WHERE c IN (SELECT c FROM ct)
);

-- 5. Actual execution still works (sanity check the pipeline is correct,
--    not just non-aborting on build).
SELECT count() FROM (
  WITH ct AS MATERIALIZED (SELECT DISTINCT c FROM t_04260)
  SELECT c FROM ct
);

DROP TABLE t_04260 SYNC;
