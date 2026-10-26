-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: `SYSTEM DROP STATISTICS CACHE` clears the server-wide cache, which would make the
-- cache hits asserted by other tests nondeterministic.
-- Tag no-parallel-replicas: the statistics are loaded on the initiator only.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- The statistics are loaded to reorder the PREWHERE conditions.
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1;
SET automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS t_05259;
DROP USER IF EXISTS user_05259;

CREATE TABLE t_05259 (p UInt8, id UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id;

INSERT INTO t_05259 SELECT 1, number FROM numbers(10000);
INSERT INTO t_05259 SELECT 2, number FROM numbers(1000);

-- The first query loads the statistics into the cache, the second one hits it.
SELECT count() FROM t_05259 WHERE id < 100 AND p = 1 SETTINGS log_comment = 'stats_cache_drop_1_cold' FORMAT Null;
SELECT count() FROM t_05259 WHERE id < 100 AND p = 1 SETTINGS log_comment = 'stats_cache_drop_2_warm' FORMAT Null;

-- The command requires its own privilege, both in the local spelling and with `ON CLUSTER`.
CREATE USER user_05259;
EXECUTE AS user_05259 SYSTEM DROP STATISTICS CACHE; -- { serverError ACCESS_DENIED }
EXECUTE AS user_05259 SYSTEM CLEAR STATISTICS CACHE; -- { serverError ACCESS_DENIED }
GRANT CLUSTER ON *.* TO user_05259;
EXECUTE AS user_05259 SYSTEM DROP STATISTICS CACHE ON CLUSTER test_shard_localhost; -- { serverError ACCESS_DENIED }

-- A denied command does not clear the cache.
SELECT count() FROM t_05259 WHERE id < 100 AND p = 1 SETTINGS log_comment = 'stats_cache_drop_3_denied' FORMAT Null;

GRANT SYSTEM DROP STATISTICS CACHE ON *.* TO user_05259;
EXECUTE AS user_05259 SYSTEM DROP STATISTICS CACHE;

-- Dropping the cache makes the next query load everything again.
SELECT count() FROM t_05259 WHERE id < 100 AND p = 1 SETTINGS log_comment = 'stats_cache_drop_4_dropped' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['LoadedStatisticsMicroseconds'] > 0 AS loaded_from_disk,
    ProfileEvents['StatisticsCacheMisses'] > 0 AS misses,
    ProfileEvents['StatisticsCacheHits'] > 0 AND ProfileEvents['StatisticsCacheMisses'] = 0 AS only_hits
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'stats_cache_drop_%'
ORDER BY log_comment;

DROP USER user_05259;
DROP TABLE t_05259;
