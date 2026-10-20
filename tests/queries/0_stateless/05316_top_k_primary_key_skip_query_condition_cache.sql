-- Tags: no-parallel
-- Tag no-parallel: the second read relies on the entries the first one wrote to the server-wide query condition cache, which other tests clear
-- The top-K granule skipping through the primary index must not stop the PREWHERE from recording the
-- granules emptied by the top-K threshold in the query condition cache: a granule skipped by the primary
-- key has all its rows beyond the threshold, so `__topKFilter` in the PREWHERE would have emptied it too.
-- Without that, a repeated `ORDER BY key LIMIT n` read never gets faster (ClickBench Q23 was 4x slower).

SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 0;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_block_size = 64;

DROP TABLE IF EXISTS t_top_k_pk_qcc;

-- `b` is the second key column; `a` is constant within most granules, so the primary key bounds `b` per granule.
CREATE TABLE t_top_k_pk_qcc (a UInt64, b UInt64, s String) ENGINE = MergeTree
ORDER BY (a, b) SETTINGS index_granularity = 64;

INSERT INTO t_top_k_pk_qcc SELECT intDiv(number, 1024), number % 1024, toString(number) FROM numbers(32768);

SELECT groupArray(b) FROM (SELECT b FROM t_top_k_pk_qcc WHERE s != 'x' ORDER BY b LIMIT 5) SETTINGS log_comment = '05316_first';
SELECT groupArray(b) FROM (SELECT b FROM t_top_k_pk_qcc WHERE s != 'x' ORDER BY b LIMIT 5) SETTINGS log_comment = '05316_second';

SYSTEM FLUSH LOGS query_log;

-- The first read skips granules by the primary key; the second one reads fewer rows thanks to the cache.
SELECT
    anyIf(ProfileEvents['TopKGranulesSkippedByPrimaryKey'], log_comment = '05316_first') > 0,
    anyIf(read_rows, log_comment = '05316_second') < anyIf(read_rows, log_comment = '05316_first')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05316_first', '05316_second')
    AND event_date >= yesterday();

DROP TABLE t_top_k_pk_qcc;
