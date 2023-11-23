-- Tags: no-parallel
-- Tag no-parallel: to decrease failure probability of collecting stack traces

-- Process one thread at a time
SET max_block_size = 1;

-- It is OK to have bigger timeout here since:
-- a) this test is marked as no-parallel
-- b) there is a filter by thread_name, so it will send signals only to the threads with the name TCPHandler
-- c) max_block_size is 1
SET storage_system_stack_trace_pipe_read_timeout_ms = 5000;

-- { echo }
SELECT count() > 0 FROM system.stack_trace WHERE query_id != '' AND thread_name = 'TCPHandler';
-- opimization for not reading /proc/self/task/{}/comm and avoid sending signal
SELECT countIf(thread_id > 0) > 0 FROM system.stack_trace;
-- optimization for trace
SELECT length(trace) > 0 FROM system.stack_trace WHERE length(trace) > 0 LIMIT 1;
-- optimization for query_id
SELECT length(query_id) > 0 FROM system.stack_trace WHERE query_id != '' AND thread_name = 'TCPHandler' LIMIT 1;
-- optimization for thread_name
SELECT length(thread_name) > 0 FROM system.stack_trace WHERE thread_name != '' LIMIT 1;
-- enough rows (optimizations works "correctly")
SELECT count() > 100 FROM system.stack_trace;
