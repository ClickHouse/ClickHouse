-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY insert deduplication against the conflict policy.
--   1. ignore: an INSERT that published nothing does not stay in the dedup log
--   2. abort: a committed replay is deduplicated, an aborted block's retry aborts again
-- Case 1 sets nothing, so it keeps the randomized async-insert coverage.

SET enable_unique_key = 1;

-- 1. ignore: red if a declined INSERT still commits its block allocation
-- (`after_reinsert` 0).
DROP TABLE IF EXISTS uk_ignore_dedup;
CREATE TABLE uk_ignore_dedup (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS unique_key_conflict_action = 'ignore', non_replicated_deduplication_window = 100,
         min_bytes_for_wide_part = 0;

INSERT INTO uk_ignore_dedup SELECT number, 'a' FROM numbers(2);

INSERT INTO uk_ignore_dedup SETTINGS insert_deduplication_token = 'tok' SELECT number, 'b' FROM numbers(2);
SELECT 'after_ignored', count(), countIf(v = 'a') FROM uk_ignore_dedup;

DELETE FROM uk_ignore_dedup WHERE id < 2;
SELECT 'after_delete', count() FROM uk_ignore_dedup;

INSERT INTO uk_ignore_dedup SETTINGS insert_deduplication_token = 'tok' SELECT number, 'b' FROM numbers(2);
SELECT 'after_reinsert', count(), countIf(v = 'b') FROM uk_ignore_dedup;

DROP TABLE uk_ignore_dedup;

-- 2. abort: red if the probe runs before the dedup check (a replay throws), or if an aborted
-- block stays registered (its byte-identical retry succeeds).
SET async_insert = 0;

DROP TABLE IF EXISTS uk_abort_replay;
CREATE TABLE uk_abort_replay (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0, non_replicated_deduplication_window = 100,
         unique_key_conflict_action = 'abort';
SYSTEM STOP MERGES uk_abort_replay;

INSERT INTO uk_abort_replay VALUES (1, 'a');
INSERT INTO uk_abort_replay VALUES (1, 'a');           -- byte-identical, so the same block id
SELECT 'replay', count(), max(v) FROM uk_abort_replay;

INSERT INTO uk_abort_replay VALUES (1, 'c'); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO uk_abort_replay VALUES (1, 'c'); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'aborted_retry', count(), max(v) FROM uk_abort_replay;

DROP TABLE uk_abort_replay;
