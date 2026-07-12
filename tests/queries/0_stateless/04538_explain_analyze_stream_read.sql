-- Tags: no-parallel-replicas, no-darwin
-- no-darwin: STREAM reads are Linux-only (server raises SUPPORT_IS_DISABLED elsewhere).
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED), which would
-- change the output.

-- EXPLAIN ANALYZE over a streaming (FROM ... STREAM) read used to abort in debug/sanitizer builds
-- with `Logical error: 'clock'`. A streaming read expands its pipeline at run time and splices in a
-- source built from a transient nested query plan; that source keeps the nested plan's ReadFromMergeTree
-- step even after the nested plan is destroyed, so it is not part of the plan walked by the EXPLAIN
-- ANALYZE per-step wall-clock registry. The `chassert(clock)` in ExecutionThreadContext wrongly assumed
-- every step-attributed processor has a registered clock. Such processors are simply not timed.

SET enable_streaming_queries = 1;
SET max_threads = 1;
SET enable_analyzer = 1;
SET optimize_distinct_in_order = 1;

DROP TABLE IF EXISTS t_explain_analyze_stream;

CREATE TABLE t_explain_analyze_stream (a String, b UInt64) ENGINE = MergeTree ORDER BY a;

-- Several parts so the streaming (commit-order) read has real work to do.
INSERT INTO t_explain_analyze_stream SELECT toString(number % 100), number FROM numbers(5000);
INSERT INTO t_explain_analyze_stream SELECT toString(number % 100), number FROM numbers(5000);
INSERT INTO t_explain_analyze_stream SELECT toString(number % 100), number FROM numbers(5000);

-- Must not abort; the actual EXPLAIN ANALYZE output is timing-dependent, so we assert structural
-- invariants via string matching on the `explain` column (as in 04312_explain_analyze).
SELECT count() > 0 FROM (EXPLAIN ANALYZE SELECT count() FROM (SELECT DISTINCT * FROM t_explain_analyze_stream STREAM LIMIT 50));
SELECT count() > 0 FROM (EXPLAIN ANALYZE SELECT * FROM t_explain_analyze_stream STREAM LIMIT 50);

-- The report is well-formed: the query summary block is present with all its fields.
SELECT
    countIf(explain LIKE '%Query summary:%') = 1,
    countIf(explain LIKE '%Time:%') = 1,
    countIf(explain LIKE '%Read:%') = 1,
    countIf(explain LIKE '%Peak memory:%') = 1
FROM (EXPLAIN ANALYZE SELECT * FROM t_explain_analyze_stream STREAM LIMIT 50);

-- The steps of the walked (outer) plan are rendered and annotated, including the streaming read step:
-- its own source processor is timed; only the processors spliced in at run time from the transient
-- nested snapshot plan are skipped.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') >= 1,
    countIf(explain LIKE '%Limit%') >= 1,
    countIf(explain LIKE '%time %') >= 1,
    countIf(explain LIKE '%parallelism%') >= 1
FROM (EXPLAIN ANALYZE SELECT * FROM t_explain_analyze_stream STREAM LIMIT 50);

DROP TABLE t_explain_analyze_stream;
