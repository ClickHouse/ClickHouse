-- Tags: no-parallel-replicas
-- no-parallel-replicas: streaming reads are always local.

SET enable_analyzer = 1;
SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_stream_spilled_round;
CREATE TABLE t_stream_spilled_round (k UInt64, v String) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         add_minmax_index_for_block_number_column = 1, add_minmax_index_for_block_offset_column = 1;
INSERT INTO t_stream_spilled_round SELECT number, toString(number) FROM numbers(20000);

-- The read round sorts by commit order and spills that sort, so the sort adds processors at run time.
-- The round is retired before the time limit ends the stream.
SELECT k FROM t_stream_spilled_round STREAM FORMAT Null
SETTINGS max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0, max_block_size = 1000, max_threads = 1,
         max_execution_time = 2, timeout_overflow_mode = 'throw', log_processors_profiles = 1, log_queries = 1; -- { serverError TIMEOUT_EXCEEDED }

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- positive control: the round did spill
SELECT ProfileEvents['ExternalSortWritePart'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND query LIKE '%FROM t_stream_spilled_round STREAM%';

-- every parent of a logged processor is itself a processor of the query
WITH (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND query LIKE '%FROM t_stream_spilled_round STREAM%') AS qid,
     (SELECT groupArray(id) FROM system.processors_profile_log WHERE query_id = qid) AS ids
SELECT countIf(NOT hasAll(ids, parent_ids)) FROM system.processors_profile_log WHERE query_id = qid;

DROP TABLE t_stream_spilled_round;
