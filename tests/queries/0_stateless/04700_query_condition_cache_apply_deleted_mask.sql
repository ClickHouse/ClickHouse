-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the cache is populated per replica, so the mark counts below are
--                       deterministic only on a single replica

-- A read with `apply_deleted_mask = 0` sees lightweight-deleted rows, so a granule whose only
-- matching rows are deleted is "no match" for an ordinary read and "may match" for that one. The two
-- therefore get separate query condition cache key spaces and must not reuse each other's entries.

SET use_query_condition_cache = 1;
-- The cache needs the analyzer on both the write and the read side.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_qcc_mask;

-- auto_statistics_types = '': randomized auto statistics would prune the whole part for the
-- never-matching predicate below, leaving nothing to read and the mark counts vacuous.
CREATE TABLE t_qcc_mask (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_qcc_mask SELECT number, number FROM numbers(100000);

-- Materialize the delete, so the mask is committed part data and nothing stays pending.
DELETE FROM t_qcc_mask WHERE id < 10 SETTINGS mutations_sync = 2;

SELECT '--- an ordinary read does not consume an apply_deleted_mask = 0 entry';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_mask WHERE v = 123456789
SETTINGS apply_deleted_mask = 0, log_comment = '04700_mask0_first';
SELECT count() FROM t_qcc_mask WHERE v = 123456789
SETTINGS log_comment = '04700_normal_after_mask0';

SELECT '--- and an apply_deleted_mask = 0 read does not consume an ordinary entry';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_mask WHERE v = 123456789
SETTINGS log_comment = '04700_normal_first';
SELECT count() FROM t_qcc_mask WHERE v = 123456789
SETTINGS apply_deleted_mask = 0, log_comment = '04700_mask0_after_normal';

SELECT '--- within its own key space, apply_deleted_mask = 0 still prunes';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_mask WHERE v = 123456789
SETTINGS apply_deleted_mask = 0, log_comment = '04700_mask0_prime';
SELECT count() FROM t_qcc_mask WHERE v = 123456789
SETTINGS apply_deleted_mask = 0, log_comment = '04700_mask0_reuse';

SYSTEM FLUSH LOGS query_log;

SELECT '--- log_comment, (any QCC hit), (read no marks at all)';
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['SelectedMarks'] = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND startsWith(log_comment, '04700_')
ORDER BY event_time_microseconds;

-- The one case the separation exists for: the deleted rows must come back for a reader that asks for
-- them, however warm the cache is for the same predicate.
SELECT '--- results stay correct with the cache warm';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_mask WHERE id < 10;
SELECT count() FROM t_qcc_mask WHERE id < 10 SETTINGS apply_deleted_mask = 0;
SELECT count() FROM t_qcc_mask WHERE id < 10;

DROP TABLE t_qcc_mask;
