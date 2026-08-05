-- Tags: no-parallel-replicas, no-darwin
-- no-darwin: STREAM reads are Linux-only (server raises SUPPORT_IS_DISABLED elsewhere).
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- EXPLAIN ANALYZE over a streaming (FROM ... STREAM) read used to abort in debug/sanitizer builds
-- with `Logical error: 'clock'`: a streaming read expands its pipeline at run time and splices in a
-- source built from a transient nested query plan, so it is not part of the plan walked by the
-- per-step wall-clock registry. Timing a streaming read (which never completes) is meaningless, so
-- `EXPLAIN ANALYZE` now rejects such queries up front, in the query tree and in the built plan (the
-- latter catches a read reached through a view, which the query tree does not expose).

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

-- Rejected: streaming read hidden behind an ordinary view. The view stays an opaque table node in the outer
-- query tree (`analyzer_inline_views` is off by default), so only the built plan exposes the streaming read.
CREATE VIEW v_explain_analyze_stream AS SELECT b FROM t_explain_analyze_stream STREAM;

EXPLAIN ANALYZE SELECT * FROM v_explain_analyze_stream LIMIT 50; -- { serverError NOT_IMPLEMENTED }

-- Rejected: same, with the view inlined into the query tree.
SET analyzer_inline_views = 1;
EXPLAIN ANALYZE SELECT * FROM v_explain_analyze_stream LIMIT 50; -- { serverError NOT_IMPLEMENTED }
SET analyzer_inline_views = 0;

-- Rejected: streaming read hidden behind a parameterized view, which is always a `StorageView`-backed table node.
CREATE VIEW pv_explain_analyze_stream AS SELECT b FROM t_explain_analyze_stream STREAM WHERE b > {lim:UInt64};

EXPLAIN ANALYZE SELECT * FROM pv_explain_analyze_stream(lim = 1) LIMIT 50; -- { serverError NOT_IMPLEMENTED }

-- Still allowed: `EXPLAIN` without `ANALYZE` does not execute, so it is not restricted.
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM v_explain_analyze_stream LIMIT 50);

DROP VIEW pv_explain_analyze_stream;
DROP VIEW v_explain_analyze_stream;
DROP TABLE t_explain_analyze_stream;

