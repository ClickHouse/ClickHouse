-- Repro for a segfault on the dedup-conflict retry path in `MergeTreeSink` (the non-replicated counterpart
-- of 04493): an async insert re-writes the temp part, but with optimize_on_insert the block can become empty
-- after the merge, leaving temp_part->part null and dereferenced later.

DROP TABLE IF EXISTS t_summing;

CREATE TABLE t_summing (k UInt32, v Int64)
ENGINE = SummingMergeTree
ORDER BY k
SETTINGS non_replicated_deduplication_window = 100;

SET async_insert = 1;
SET wait_for_async_insert = 0;
SET async_insert_deduplicate = 1;
SET optimize_on_insert = 1;
-- Disable adaptive timeout and make the fixed timeout large, so nothing is flushed before SYSTEM FLUSH ASYNC INSERT QUEUE.
SET async_insert_use_adaptive_busy_timeout = 0;
SET async_insert_busy_timeout_max_ms = 1000000;

-- 1) Register the block id for token t1, flushed alone.
SET insert_deduplication_token = 't1';
INSERT INTO t_summing VALUES (1, 10);

SYSTEM FLUSH ASYNC INSERT QUEUE t_summing;

-- 2) Two async inserts batched together (the dedup token is set on the session level
--    because it is excluded from the batch key, unlike a SETTINGS clause in the query):
--       t1: (1, 10)          -> conflicts with the block id from step 1
--       t2: (2, 5), (2, -5)  -> fresh, sums to zero, so optimize_on_insert drops the whole part
SET insert_deduplication_token = 't1';
INSERT INTO t_summing VALUES (1, 10);
SET insert_deduplication_token = 't2';
INSERT INTO t_summing VALUES (2, 5), (2, -5);

SYSTEM FLUSH ASYNC INSERT QUEUE t_summing;

-- The server must still be alive and the state consistent: t1 deduplicated, t2 summed to zero and dropped.
SELECT groupArray((k, v)) FROM t_summing;

DROP TABLE t_summing;
