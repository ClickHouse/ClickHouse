-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY insert deduplication against the conflict policy.
--   2. abort: a committed replay is deduplicated, an aborted block's retry aborts again

SET enable_unique_key = 1;

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
