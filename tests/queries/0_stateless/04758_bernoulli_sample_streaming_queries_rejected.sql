-- Bernoulli sampling (SAMPLE on a MergeTree table without a SAMPLE BY key) must be rejected
-- in combination with STREAM.

DROP TABLE IF EXISTS t_bernoulli_stream;

CREATE TABLE t_bernoulli_stream (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli_stream SELECT number FROM numbers(1000);

SELECT 'bernoulli sample with STREAM is rejected';
-- A streaming read goes through makeStreamingSelectQueryInfo, which clears
-- table_expression_modifiers, and ReadFromMergeTree::selectRangesToRead returns before getSampling,
-- so the per-part Bernoulli filter is never built in the streaming subplan. SAMPLE + STREAM would
-- therefore silently return the full (unsampled) table. The analyzer must reject the combination up
-- front. With the analyzer on Linux this is SYNTAX_ERROR (the modifier-compatibility guard in
-- validateTableExpressionModifiers); on non-Linux platforms STREAM itself is unsupported, so
-- SUPPORT_IS_DISABLED fires first. With the old analyzer `validateTableExpressionModifiers` never
-- runs, but streaming reads are not implemented there at all (`InterpreterSelectQuery`), so
-- NOT_IMPLEMENTED fires instead. Every path rejects the combination up front, which is what matters.
SELECT count() FROM t_bernoulli_stream SAMPLE 0.1 STREAM
SETTINGS allow_experimental_bernoulli_sample = 1, enable_streaming_queries = 1; -- { serverError SYNTAX_ERROR, SUPPORT_IS_DISABLED, NOT_IMPLEMENTED }

DROP TABLE t_bernoulli_stream;
