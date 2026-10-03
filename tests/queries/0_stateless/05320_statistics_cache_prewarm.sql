-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- Tag no-parallel-replicas: the statistics are loaded on the initiator only.

-- With `prewarm_statistics_cache`, the statistics of a new part and of all parts at startup are
-- loaded into the statistics cache, so the first query after an insert or after attaching the table
-- reads nothing from disk.

SET enable_analyzer = 1;
SET use_statistics = 1;
SET use_statistics_for_part_pruning = 0;
SET materialize_statistics_on_insert = 1;
SET optimize_move_to_prewhere = 1;
SET use_query_cache = 0;
-- One part per insert: a merge right after the insert would race with the first query.
SET max_insert_threads = 1;

DROP TABLE IF EXISTS t_05320;

CREATE TABLE t_05320 (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic, uniq_v2', prewarm_statistics_cache = 1, min_bytes_to_prewarm_caches = 0;

INSERT INTO t_05320 SELECT number, number % 100 FROM numbers(10000);

SELECT sum(v) FROM t_05320 WHERE k % 3 = 0 AND v % 2 = 0
SETTINGS log_comment = 'prewarm_05320_after_insert' FORMAT Null;

DETACH TABLE t_05320;
ATTACH TABLE t_05320;

SELECT sum(v) FROM t_05320 WHERE k % 3 = 0 AND v % 2 = 0
SETTINGS log_comment = 'prewarm_05320_after_attach' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['LoadedStatisticsMicroseconds'] AS loaded_from_disk,
    ProfileEvents['StatisticsCacheHits'] > 0 AS hits
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'prewarm_05320_%'
ORDER BY log_comment;

DROP TABLE t_05320;
