-- Tags: atomic-database, no-parallel
-- At-least-once for incremental refreshable MV: if a refresh appends its snapshot but fails before the
-- advanced cursor is persisted, the next refresh replays from the old cursor and reprocesses the round
-- (duplicating rows) rather than losing them. Driven by the refresh_mv_incremental_fail_after_append
-- failpoint (global server state -> no-parallel).

DROP TABLE IF EXISTS alo_src;
DROP TABLE IF EXISTS alo_tgt;
DROP TABLE IF EXISTS alo_mv;

CREATE TABLE alo_src (k UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

CREATE TABLE alo_tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

-- refresh_retries = 0 so a failed refresh does not auto-retry; every refresh below is triggered manually.
CREATE MATERIALIZED VIEW alo_mv
    REFRESH EVERY 10 YEAR SETTINGS refresh_incremental = 1, refresh_retries = 0 APPEND
    TO alo_tgt EMPTY
    AS SELECT k FROM alo_src;

-- Round 1: commit rows 0..4 and refresh normally; the cursor advances past them.
INSERT INTO alo_src SELECT number FROM numbers(5);
SYSTEM REFRESH VIEW alo_mv;
SYSTEM WAIT VIEW alo_mv;
SELECT 'round1', count(), uniqExact(k) FROM alo_tgt;

-- Round 2: commit rows 5..9, then fail the refresh AFTER the append but BEFORE the cursor is persisted.
-- The appended rows land in the target (count grows), but the cursor is not advanced.
INSERT INTO alo_src SELECT number FROM numbers(5, 5);
SYSTEM ENABLE FAILPOINT refresh_mv_incremental_fail_after_append;
SYSTEM REFRESH VIEW alo_mv;
SYSTEM WAIT VIEW alo_mv; -- { serverError REFRESH_FAILED }
SYSTEM DISABLE FAILPOINT refresh_mv_incremental_fail_after_append;
SELECT 'after_fail', count(), uniqExact(k) FROM alo_tgt;

-- Round 3: refresh again. The cursor was not advanced, so rows 5..9 are reprocessed and appended a second
-- time: nothing is lost (uniqExact stays 10) but rows are duplicated (count = 15). That is at-least-once.
SYSTEM REFRESH VIEW alo_mv;
SYSTEM WAIT VIEW alo_mv;
SELECT 'after_recover', count(), uniqExact(k) FROM alo_tgt;

DROP TABLE alo_mv;
DROP TABLE alo_tgt;
DROP TABLE alo_src;
