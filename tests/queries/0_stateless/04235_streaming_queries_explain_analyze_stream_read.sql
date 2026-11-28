-- Tags: no-parallel-replicas, no-darwin
-- no-darwin: STREAM reads are Linux-only (server raises SUPPORT_IS_DISABLED elsewhere).
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- A streaming (`FROM ... STREAM`) read expands its pipeline at run time and splices in a sub-pipeline
-- built from a transient nested query plan. Those processors are attributed to the owning read step,
-- so `EXPLAIN ANALYZE` reports them under `ReadFromMergeTree`. The `LIMIT` is what makes the read
-- terminate: a streaming read stops once its output port finishes.

SET enable_streaming_queries = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_explain_analyze_stream;

CREATE TABLE t_explain_analyze_stream (a String, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_explain_analyze_stream SELECT toString(number % 100), number FROM numbers(5000);

SELECT count() > 0 FROM
    (EXPLAIN ANALYZE SELECT * FROM t_explain_analyze_stream STREAM LIMIT 50)
    WHERE explain LIKE '%ReadFromMergeTree%';

DROP TABLE t_explain_analyze_stream;
