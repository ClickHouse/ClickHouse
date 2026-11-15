-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY insert deduplication against the conflict policy.
--   2. abort: a committed replay is deduplicated, an aborted block's retry aborts again
--   3. coalesced flush: the rows left once a replayed token is dropped are written

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

-- 3. coalesced flush: red if the rewrite of the rows left after dropping the replayed token reuses
-- the rolled-back transaction, so the flush fails and row 2 is lost (`coalesced` without it).
-- One explicit flush coalesces both queued entries: the queue key ignores the token, but not a
-- SETTINGS clause in the query text, hence SET.
SET async_insert = 1, wait_for_async_insert = 0, async_insert_busy_timeout_min_ms = 600000,
    async_insert_busy_timeout_max_ms = 600000, async_insert_use_adaptive_busy_timeout = 0, insert_deduplicate = 1;

DROP TABLE IF EXISTS uk_coalesced;
CREATE TABLE uk_coalesced (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS non_replicated_deduplication_window = 100;

SET insert_deduplication_token = 'A';
INSERT INTO uk_coalesced VALUES (1, 'a');
SYSTEM FLUSH ASYNC INSERT QUEUE uk_coalesced;

INSERT INTO uk_coalesced VALUES (1, 'a');
SET insert_deduplication_token = 'B';
INSERT INTO uk_coalesced VALUES (2, 'b');
SYSTEM FLUSH ASYNC INSERT QUEUE uk_coalesced;

SELECT 'coalesced', groupArray((id, v)) FROM (SELECT id, v FROM uk_coalesced ORDER BY id);

DROP TABLE uk_coalesced;
