-- Tags: no-parallel
-- Tag no-parallel: relies on the entries of the instance-wide query condition cache, which other tests clear.

-- The granules of a `Memory` table skipped by the query condition cache are not scanned, so they are
-- not counted as read and do not count against `max_rows_to_read`.

SET use_query_condition_cache = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS t_memory_qcc_read_rows;
CREATE TABLE t_memory_qcc_read_rows (k UInt64, x UInt8) ENGINE = Memory;
-- Three blocks of 30000 rows; the matching rows are in the second granule of the second block.
INSERT INTO t_memory_qcc_read_rows SELECT number, number BETWEEN 40000 AND 40009 FROM numbers(90000) SETTINGS max_block_size = 30000, max_insert_threads = 1;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT count() FROM t_memory_qcc_read_rows WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1, log_comment = '05366_prewhere_1';
SELECT count() FROM t_memory_qcc_read_rows WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1, log_comment = '05366_prewhere_2';
-- Only the granule with matches is scanned.
SELECT count() FROM t_memory_qcc_read_rows WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1, max_rows_to_read = 10000, read_overflow_mode = 'throw';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT count() FROM t_memory_qcc_read_rows WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05366_where_1';
SELECT count() FROM t_memory_qcc_read_rows WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05366_where_2';
SELECT count() FROM t_memory_qcc_read_rows WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0, max_rows_to_read = 10000, read_overflow_mode = 'throw';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05366_prewhere_1', '05366_prewhere_2', '05366_where_1', '05366_where_2') AND event_date >= yesterday()
ORDER BY log_comment;

DROP TABLE t_memory_qcc_read_rows;
