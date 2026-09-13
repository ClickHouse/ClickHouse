-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: messes with the server-level query condition cache
-- Tag no-parallel-replicas: `tryOptimizeTopK` bails out on a distributed plan

-- `ORDER BY col LIMIT n` is served by a dynamic `__topKFilter` PREWHERE whose running threshold
-- empties whole granules. Those granules used to be recomputed on every execution: the query
-- condition cache write in `MergeTreeSelectProcessor::read` was gated on the PREWHERE predicate
-- being deterministic, which `__topKFilter` deliberately is not. They are now recorded under the
-- key of the query's whole filter, salted with the TopK plan parameters and the part set.

DROP TABLE IF EXISTS t_topk_qcc;

CREATE TABLE t_topk_qcc (k UInt64, v UInt64)
ENGINE = MergeTree
ORDER BY k
-- Pinned so that CI setting randomization cannot change the granule count this test reasons about.
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- 245 granules, `v` ascending with the physical order, so the threshold reaches its final value
-- while the first block is being sorted and every later granule is emptied by `__topKFilter`.
INSERT INTO t_topk_qcc SELECT number, number FROM numbers(2000000);

SYSTEM DROP QUERY CONDITION CACHE;

-- `max_threads = 1` so the threshold trajectory, and with it the set of emptied granules, does not
-- depend on how the marks happen to be spread over the reading threads.
SELECT v FROM t_topk_qcc ORDER BY v LIMIT 3
SETTINGS max_threads = 1, max_block_size = 65536, use_query_condition_cache = 1,
         use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         log_comment = '05088_run1';

-- Same query, same answer, but now the emptied granules come from the cache.
SELECT v FROM t_topk_qcc ORDER BY v LIMIT 3
SETTINGS max_threads = 1, max_block_size = 65536, use_query_condition_cache = 1,
         use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         log_comment = '05088_run2';

-- A TopK query over a different set of rows must not reuse those granule decisions: its own top 3
-- lives in granules that the query above emptied. The bare `__topKFilter(v)` predicate hashes the
-- same for both, so this only holds because the key covers the whole filter.
SELECT v FROM t_topk_qcc WHERE k % 500000 = 499999 ORDER BY v LIMIT 3
SETTINGS max_threads = 1, max_block_size = 65536, use_query_condition_cache = 1,
         use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1;

-- And a plain aggregate over the same column still sees every row.
SELECT count(), max(v) FROM t_topk_qcc SETTINGS use_query_condition_cache = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['SelectedMarks'] = ProfileEvents['SelectedMarksTotal'] AS read_every_granule,
    ProfileEvents['SelectedMarks'] * 2 < ProfileEvents['SelectedMarksTotal'] AS skipped_most_granules
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05088_run1', '05088_run2')
-- Keep only the latest execution per query, so that a retry of this test in the same database
-- (clickhouse-test reuses it) does not double the output.
ORDER BY log_comment, event_time_microseconds DESC
LIMIT 1 BY log_comment;

DROP TABLE t_topk_qcc;
