-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: `tryOptimizeTopK` bails out on a distributed plan

-- `tryOptimizeTopK` also marks an `ORDER BY ... LIMIT n` read that only uses a minmax skip index on the
-- sort column, without the dynamic `__topKFilter` PREWHERE. When such a query has a deterministic user
-- PREWHERE, the granules that PREWHERE empties must be recorded in the query condition cache under the
-- PREWHERE predicate's own hash, as for any other query, and not under the TopK-salted key of the whole
-- filter - otherwise a plain query with the same PREWHERE cannot reuse them.

-- Pinned so that CI setting randomization cannot change which path the TopK query takes:
-- no dynamic filtering, skip index considered, and no read-time skip-index reader in front of
-- PREWHERE (that disables the PREWHERE write altogether).
SET use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0,
    query_plan_max_limit_for_top_k_optimization = 1000, use_query_condition_cache = 1,
    use_query_condition_cache_for_top_k = 1;

DROP TABLE IF EXISTS t_topk_skip_qcc;

CREATE TABLE t_topk_skip_qcc (k UInt64, v UInt64, w UInt8, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- `w = 1` only in the first granule, so PREWHERE empties all the others.
INSERT INTO t_topk_skip_qcc SELECT number, 1000000 - number, number < 8192 FROM numbers(1000000);

SELECT v FROM t_topk_skip_qcc PREWHERE w = 1 ORDER BY v LIMIT 3 SETTINGS log_comment = '05243_topk';

-- The same PREWHERE without TopK finds the granules the query above emptied.
SELECT count() FROM t_topk_skip_qcc PREWHERE w = 1 SETTINGS log_comment = '05243_plain';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0 AS cache_hit
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05243_topk', '05243_plain')
ORDER BY log_comment, event_time_microseconds DESC
LIMIT 1 BY log_comment;

DROP TABLE t_topk_skip_qcc;
