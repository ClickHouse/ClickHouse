-- Tags: no-parallel
-- Tag no-parallel: checks the hits of the instance-wide query condition cache, which other tests clear.

-- A query over a `Memory` table records the granules without matches in the query condition cache,
-- and the next query with the same condition skips them.

SET use_query_condition_cache = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS t_memory_qcc_hits;
CREATE TABLE t_memory_qcc_hits (k UInt64, x UInt8, s String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_qcc_hits SELECT number, number BETWEEN 40000 AND 40009, toString(number) FROM numbers(90000) SETTINGS max_block_size = 30000;

SYSTEM DROP QUERY CONDITION CACHE;

-- The condition stays in the filter step above the read, which records the granules.
SELECT count() FROM t_memory_qcc_hits WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05323_where_1';
SELECT count() FROM t_memory_qcc_hits WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05323_where_2';

SYSTEM DROP QUERY CONDITION CACHE;

-- The condition is in PREWHERE, which the source evaluates and records itself.
SELECT count(), sum(length(s)) FROM t_memory_qcc_hits WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1, log_comment = '05323_prewhere_1';
SELECT count(), sum(length(s)) FROM t_memory_qcc_hits WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1, log_comment = '05323_prewhere_2';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0, ProfileEvents['QueryConditionCacheMisses'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05323\_%' AND event_date >= yesterday()
ORDER BY log_comment;

DROP TABLE t_memory_qcc_hits;
