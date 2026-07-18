-- Tags: no-parallel-replicas, no-darwin
-- no-darwin: STREAM reads are Linux-only (server raises SUPPORT_IS_DISABLED elsewhere).
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- EXPLAIN ANALYZE over a streaming (FROM ... STREAM) read used to abort in debug/sanitizer builds
-- with `Logical error: 'clock'`: a streaming read expands its pipeline at run time and splices in a
-- source built from a transient nested query plan, so it is not part of the plan walked by the
-- per-step wall-clock registry. Timing a streaming read (which never completes) is meaningless, so
-- EXPLAIN ANALYZE now rejects such queries up front at the query-tree level.

SET enable_streaming_queries = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_explain_analyze_stream;

CREATE TABLE t_explain_analyze_stream (a String, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_explain_analyze_stream SELECT toString(number % 100), number FROM numbers(5000);

-- Rejected: streaming read directly.
EXPLAIN ANALYZE SELECT * FROM t_explain_analyze_stream STREAM LIMIT 50; -- { serverError NOT_IMPLEMENTED }

-- Rejected: streaming read nested in a join-tree subquery.
EXPLAIN ANALYZE SELECT * FROM (SELECT * FROM t_explain_analyze_stream STREAM) LIMIT 50; -- { serverError NOT_IMPLEMENTED }

-- Rejected: streaming read nested in a WHERE IN subquery. The check used to only walk join-tree table
-- expressions, so a streaming read outside the join tree slipped through and aborted with 'clock'.
EXPLAIN ANALYZE SELECT a FROM t_explain_analyze_stream WHERE b IN (SELECT b FROM t_explain_analyze_stream STREAM); -- { serverError NOT_IMPLEMENTED }

-- Rejected: streaming read inside an INTERSECT ALL nested in a WHERE IN subquery (the AST-fuzzer shape).
EXPLAIN ANALYZE SELECT a FROM t_explain_analyze_stream WHERE b IN (SELECT b FROM t_explain_analyze_stream INTERSECT ALL SELECT b FROM t_explain_analyze_stream STREAM); -- { serverError NOT_IMPLEMENTED }

-- Rejected: streaming read in a CTE referenced from a WHERE IN subquery.
EXPLAIN ANALYZE WITH cte AS (SELECT b FROM t_explain_analyze_stream STREAM) SELECT a FROM t_explain_analyze_stream WHERE b IN (SELECT b FROM cte); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_explain_analyze_stream;

