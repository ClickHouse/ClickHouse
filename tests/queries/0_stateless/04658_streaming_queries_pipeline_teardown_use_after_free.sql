-- Tags: no-parallel-replicas, no-darwin
-- no-darwin: STREAM reads are Linux-only (server raises SUPPORT_IS_DISABLED elsewhere).
-- no-parallel-replicas: streaming reads are always local.

-- Regression test for a heap-use-after-free in `ExecutingGraph::updateNode` found by the AST fuzzer.
-- A streaming query runs one `MergeTreeCommitOrderSequentialSource` per stream, all feeding a single
-- `LimitTransform`, and every source repeatedly attaches and removes its snapshot-reading sub-pipeline
-- at run time. Removing the sub-pipeline freed graph edges whose pointers were still queued in
-- `updateNode`'s local work-list, and a later pop dereferenced freed memory.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=100535&sha=0fb86507fb4817cb2418012cce69069f92e74972&name_0=PR&name_1=AST%20fuzzer%20%28arm_asan_ubsan%29

SET enable_streaming_queries = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_stream_pipeline_teardown;

CREATE TABLE t_stream_pipeline_teardown (k UInt64, v String)
ENGINE = MergeTree
PARTITION BY k % 16
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, add_minmax_index_for_block_number_column = 1, add_minmax_index_for_block_offset_column = 1;

INSERT INTO t_stream_pipeline_teardown SELECT number, toString(number) FROM numbers(5000);
INSERT INTO t_stream_pipeline_teardown SELECT number, toString(number) FROM numbers(5000, 5000);

-- The fuzzed query: the cursor skips partition '0', `LIMIT 909, 257` terminates the stream once satisfied,
-- and `max_threads = 16` gives 16 streaming sources whose sub-pipelines are torn down concurrently.
SELECT toFixedString('^$', 65536), toInt128(-2)
FROM t_stream_pipeline_teardown
STREAM CURSOR {'0': {'block_number': 10, 'block_offset': 1000000}}
WHERE toLowCardinality(1048577)
LIMIT 909, 257
SETTINGS max_threads = 16
FORMAT Null;

DROP TABLE t_stream_pipeline_teardown;
