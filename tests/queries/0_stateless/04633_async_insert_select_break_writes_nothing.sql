-- A user INSERT ... SELECT under async_insert whose SELECT is stopped by a break-mode
-- max_execution_time must write nothing: cancelling the SELECT tears down the queue transform
-- before it can divert the buffered block. This locks in that behaviour.
DROP TABLE IF EXISTS t_04633_break_writes_nothing;
CREATE TABLE t_04633_break_writes_nothing (n UInt8) ENGINE = MergeTree ORDER BY n;

-- `number = 0` passes immediately (the OR short-circuits the sleep) and is buffered for the queue
-- route; the later rows sleep and are filtered out, so the one-second break fires with a block
-- still held. Nothing is written.
INSERT INTO t_04633_break_writes_nothing
SELECT number FROM numbers(6) WHERE number = 0 OR sleepEachRow(0.5) = 99
SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_select_as_async_insert = 1,
         max_block_size = 1, max_threads = 1,
         function_sleep_max_microseconds_per_block = 60000000,
         max_execution_time = 1, timeout_overflow_mode = 'break';

SELECT count() FROM t_04633_break_writes_nothing;

-- Positive control: without a time limit the same eligible query writes its block.
INSERT INTO t_04633_break_writes_nothing
SELECT number FROM numbers(4)
SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_select_as_async_insert = 1,
         max_block_size = 4, max_threads = 1;

SELECT count() FROM t_04633_break_writes_nothing;

DROP TABLE t_04633_break_writes_nothing;
