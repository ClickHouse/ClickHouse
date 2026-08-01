-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the query condition cache is populated per replica, so the granule
--                       accounting below is deterministic only on a single replica

-- A materialized lightweight delete must not disable the query condition cache.
--
-- The step that applies the `_row_exists` mask is appended to the read task's `mutation_steps`,
-- and the cache write path used to treat a non-empty `mutation_steps` as "a mutation filtered rows
-- before PREWHERE, do not attribute anything to the predicate". A materialized mask is committed
-- part data rather than a pending mutation, so that check disabled the cache for every table that
-- had ever been touched by a lightweight delete - a single deleted row was enough.

SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS t_qcc_lwd;

CREATE TABLE t_qcc_lwd (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_qcc_lwd SELECT number, number FROM numbers(1000000);

-- Materialize the delete so the part carries `_row_exists` and no mutation is left pending.
DELETE FROM t_qcc_lwd WHERE id = 0 SETTINGS mutations_sync = 2;

SELECT '--- the delete is materialized, nothing pending';
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_qcc_lwd' AND NOT is_done;

SYSTEM DROP QUERY CONDITION CACHE;

-- `v = 123456789` matches no row, so after the first (priming) run every granule is known not to
-- match and the second run must read strictly fewer marks.
SELECT count() FROM t_qcc_lwd WHERE v = 123456789 SETTINGS log_comment = '04669_lwd_prime';
SELECT count() FROM t_qcc_lwd WHERE v = 123456789 SETTINGS log_comment = '04669_lwd_reuse';

SYSTEM FLUSH LOGS query_log;

SELECT '--- prime reads everything, reuse prunes';
-- Columns: (any QCC hit), (granules skipped). Expected: prime = 0 0, reuse = 1 1.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04669_lwd_prime', '04669_lwd_reuse')
ORDER BY event_time_microseconds;

-- An unmaterialized (on-fly) lightweight delete still varies per query, so it must keep the cache
-- write disabled. That direction is covered by 03229_query_condition_cache_on_fly_mutations.

SELECT '--- apply_deleted_mask = 0 must not consume entries written by a normal read';
-- Prime with a predicate that only the deleted row satisfies. A normal read sees no match and may
-- record the granule as non-matching; a later `apply_deleted_mask = 0` read must still return the
-- deleted row rather than reuse that verdict.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_lwd WHERE id = 0;
SELECT count() FROM t_qcc_lwd WHERE id = 0 SETTINGS apply_deleted_mask = 0;

SELECT '--- and the reverse direction is also unaffected';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_lwd WHERE id = 0 SETTINGS apply_deleted_mask = 0;
SELECT count() FROM t_qcc_lwd WHERE id = 0;

SELECT '--- results stay correct with the cache warm';
SELECT count() FROM t_qcc_lwd;
SELECT count() FROM t_qcc_lwd WHERE v < 10;

DROP TABLE t_qcc_lwd;
