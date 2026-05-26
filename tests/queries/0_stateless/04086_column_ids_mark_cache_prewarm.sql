-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
-- Regression: SYSTEM PREWARM MARK CACHE on a column whose column_id is
-- non-identity (DROP + same-name ADD) must actually prewarm the cache.
-- Before the fix, MergeTreeDataPartWide::loadMarksToCache used the bare-
-- string overload of getStreamNameForColumn, which derives the stream
-- name from the logical column name; the real file lives under the
-- numeric column ID, so prewarm silently looked up the wrong key and
-- did nothing — the next SELECT paid full mark-load I/O.

SET allow_experimental_column_ids = 1;

DROP TABLE IF EXISTS t_prewarm_column_ids;

CREATE TABLE t_prewarm_column_ids (a UInt64, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         prewarm_mark_cache = 0;

-- Push c's column_id off identity so 'c' on disk is `1.cmrk2`, not `c.cmrk2`.
INSERT INTO t_prewarm_column_ids VALUES (1, 1, 1);
ALTER TABLE t_prewarm_column_ids DROP COLUMN c;
ALTER TABLE t_prewarm_column_ids ADD COLUMN c UInt64;
INSERT INTO t_prewarm_column_ids VALUES (2, 2, 2);

-- Sanity check: c is on a non-identity column_id.
SELECT 'is_non_identity', column_id != column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_prewarm_column_ids' AND active AND column = 'c'
ORDER BY name DESC LIMIT 1;

-- Round 1: cold path baseline.  CLEAR + SELECT loads marks for `c` from
-- disk because the mark cache was just cleared.  Expected: LoadedMarksCount > 0.
SYSTEM CLEAR MARK CACHE;
SELECT count() FROM t_prewarm_column_ids WHERE c > 0;

-- Round 2: warm path via prewarm.  CLEAR, then PREWARM, then SELECT.
-- Before the fix, prewarm was a no-op on the non-identity `c` column;
-- the SELECT still triggered cold mark loads.  After the fix, the cache
-- is populated and LoadedMarksCount must be 0 for the SELECT.
SYSTEM CLEAR MARK CACHE;
SYSTEM PREWARM MARK CACHE t_prewarm_column_ids;
SELECT count() FROM t_prewarm_column_ids WHERE c > 0;

SYSTEM FLUSH LOGS query_log;

-- Two SELECTs in order: the first (post-CLEAR) should report LoadedMarksCount > 0;
-- the second (post-PREWARM) must report LoadedMarksCount = 0.
SELECT ProfileEvents['LoadedMarksCount'] > 0 AS loaded_marks_after_clear FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE 'SELECT count() FROM t_prewarm_column_ids WHERE c > 0%'
ORDER BY event_time_microseconds LIMIT 1;

SELECT ProfileEvents['LoadedMarksCount'] AS loaded_marks_after_prewarm FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE 'SELECT count() FROM t_prewarm_column_ids WHERE c > 0%'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_prewarm_column_ids;
