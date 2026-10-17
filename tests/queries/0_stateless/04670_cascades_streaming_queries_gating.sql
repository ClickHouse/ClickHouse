-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A `STREAM` read cannot be serialized into a distributed plan fragment, so distributed Cascades
-- planning rejects it up front. Lives outside `04330_cascades_activation_gating` only because a
-- test that uses `enable_streaming_queries` must carry `_streaming_queries_` in its name.

DROP TABLE IF EXISTS t_stream_gating;
CREATE TABLE t_stream_gating (k UInt32, x Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_stream_gating SELECT number % 10, number FROM numbers(100);

SELECT '-- A STREAM read is rejected (fail-close)';
SELECT count() FROM t_stream_gating STREAM
SETTINGS enable_streaming_queries = 1, enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_stream_gating;
