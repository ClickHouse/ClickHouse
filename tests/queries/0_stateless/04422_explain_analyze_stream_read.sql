-- Tags: no-parallel-replicas, no-darwin
-- no-darwin: STREAM reads are Linux-only (server raises SUPPORT_IS_DISABLED elsewhere).
-- EXPLAIN ANALYZE does not support Streaming Queries hence this test is added

SET enable_streaming_queries = 1;
SET max_threads = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_stream_explain_analyze;

CREATE TABLE t_stream_explain_analyze (a String, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_stream_explain_analyze SELECT toString(number % 100), number FROM numbers(5000);
INSERT INTO t_stream_explain_analyze SELECT toString(number % 100), number FROM numbers(5000);

EXPLAIN ANALYZE SELECT count() FROM (SELECT DISTINCT a FROM t_stream_explain_analyze STREAM LIMIT 50); -- { serverError NOT_IMPLEMENTED }
EXPLAIN ANALYZE actions = 1, projections = 1, sorting = 1, input_headers = 1 SELECT count() FROM (SELECT DISTINCT * FROM t_stream_explain_analyze STREAM LIMIT 50); -- { serverError ILLEGAL_STREAM }

DROP TABLE t_stream_explain_analyze;
