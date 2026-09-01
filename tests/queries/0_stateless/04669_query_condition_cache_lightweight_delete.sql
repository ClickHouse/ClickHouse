-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the query condition cache is populated per replica, so the granule
--                       accounting below is deterministic only on a single replica

-- A materialized lightweight delete must not disable the query condition cache. The cache write
-- path used to skip any part with a non-empty `mutation_steps`, which also holds the step applying
-- the committed `_row_exists` mask - so one deleted row disabled the cache for the whole table.

SET use_query_condition_cache = 1;
-- The cache needs the analyzer on both the write and the read side.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_qcc_lwd;

-- auto_statistics_types = '': randomized auto statistics would prune the whole part for the
-- never-matching predicates below, leaving nothing to read and the granule counts below vacuous.
CREATE TABLE t_qcc_lwd (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_qcc_lwd SELECT number, number FROM numbers(1000000);

-- Materialize the delete so the part carries `_row_exists` and no mutation is left pending.
DELETE FROM t_qcc_lwd WHERE id = 0 SETTINGS mutations_sync = 2;

SELECT '--- the delete is materialized, nothing pending';
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_qcc_lwd' AND NOT is_done;

SYSTEM DROP QUERY CONDITION CACHE;

-- `v = 123456789` matches no row, so after the first (priming) run every granule of every part is
-- known not to match and the second run must read no marks at all. Asserting zero rather than
-- "fewer" matters: only the part holding `id = 0` carries the mask, so if the insert ever lands in
-- more than one part, a weaker assertion would be satisfied by the other parts pruning.
SELECT count() FROM t_qcc_lwd WHERE v = 123456789 SETTINGS log_comment = '04669_lwd_prime';
SELECT count() FROM t_qcc_lwd WHERE v = 123456789 SETTINGS log_comment = '04669_lwd_reuse';

SYSTEM FLUSH LOGS query_log;

SELECT '--- prime reads everything, reuse prunes';
-- Columns: (any QCC hit), (read no marks at all). Expected: prime = 0 0, reuse = 1 1.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['SelectedMarks'] = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04669_lwd_prime', '04669_lwd_reuse')
ORDER BY event_time_microseconds;

-- The unmaterialized (on-fly) direction, which must keep the cache write disabled, is covered by
-- 03229_query_condition_cache_on_fly_mutations.

SELECT '--- apply_deleted_mask = 0 must not consume entries written by a normal read';
-- `id = 0` matches only the deleted row, so a normal read may record the granule as non-matching.
-- An `apply_deleted_mask = 0` read must still return that row instead of reusing the verdict.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_lwd WHERE id = 0;
SELECT count() FROM t_qcc_lwd WHERE id = 0 SETTINGS apply_deleted_mask = 0;

SELECT '--- and the reverse direction is also unaffected';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_lwd WHERE id = 0 SETTINGS apply_deleted_mask = 0;
SELECT count() FROM t_qcc_lwd WHERE id = 0;

SELECT '--- apply_deleted_mask = 0 neither writes nor consumes the cache';
-- Such queries are kept out of the cache entirely instead of getting their own key space, so a
-- repeated `apply_deleted_mask = 0` query does not prune. Both runs must miss and read every mark.
-- Pinned here because it is the one behaviour this change gives up; a follow-up that keys entries by
-- `apply_deleted_mask` instead of disabling them has to update this block deliberately.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_lwd WHERE v = 123456789
SETTINGS apply_deleted_mask = 0, log_comment = '04669_lwd_mask0_prime';
SELECT count() FROM t_qcc_lwd WHERE v = 123456789
SETTINGS apply_deleted_mask = 0, log_comment = '04669_lwd_mask0_reuse';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['SelectedMarks'] = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04669_lwd_mask0_prime', '04669_lwd_mask0_reuse')
ORDER BY event_time_microseconds;

SELECT '--- results stay correct with the cache warm';
SELECT count() FROM t_qcc_lwd;
SELECT count() FROM t_qcc_lwd WHERE v < 10;

DROP TABLE t_qcc_lwd;

-- A pending on-fly mutation of a column the query does not read produces no read step, so it must
-- not disable the cache write either. Only an `apply_mutations_on_fly = 0` query can consume the
-- entry: the read path skips the cache while a data mutation is pending.

SELECT '--- a pending mutation on an unread column must not disable the cache';

DROP TABLE IF EXISTS t_qcc_lwd_pending;

CREATE TABLE t_qcc_lwd_pending (id UInt64, v UInt64, w UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_qcc_lwd_pending SELECT number, number, number FROM numbers(1000000);

DELETE FROM t_qcc_lwd_pending WHERE id = 0 SETTINGS mutations_sync = 2;

SYSTEM STOP MERGES t_qcc_lwd_pending;
ALTER TABLE t_qcc_lwd_pending UPDATE w = 0 WHERE id = 1 SETTINGS mutations_sync = 0;

SYSTEM DROP QUERY CONDITION CACHE;

-- The prime reads only `v`, so the pending `UPDATE` of `w` is irrelevant to it and the write must
-- still happen; the reuse with `apply_mutations_on_fly = 0` must consume it and prune.
SELECT count() FROM t_qcc_lwd_pending WHERE v = 123456789
SETTINGS apply_mutations_on_fly = 1, log_comment = '04669_lwd_pending_prime';
SELECT count() FROM t_qcc_lwd_pending WHERE v = 123456789
SETTINGS apply_mutations_on_fly = 0, log_comment = '04669_lwd_pending_reuse';

SYSTEM FLUSH LOGS query_log;

SELECT '--- pending-mutation prime reads everything, reuse prunes';
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['SelectedMarks'] = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04669_lwd_pending_prime', '04669_lwd_pending_reuse')
ORDER BY event_time_microseconds;

DROP TABLE t_qcc_lwd_pending;
