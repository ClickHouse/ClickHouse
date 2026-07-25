-- Tags: no-fasttest
-- no-fasttest: needs the async insert queue to coalesce several entries into one flush.

-- Two identical async-insert entries (no user token, data-hash path), each spanning two
-- partitions, coalesced into one flush. Self-deduplication must drop the second token, and
-- the per-partition block rewrite must keep only the current partition's rows: the
-- deduplication-retry chain ends with `SelectPartitionTransform`, so a row of another
-- partition must never leak into this partition's part.

DROP TABLE IF EXISTS dedup_identical_multi_partition;

CREATE TABLE dedup_identical_multi_partition (p UInt8, x UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS non_replicated_deduplication_window = 1000;

-- First flush: two identical entries, self-deduplicated within the flush.
INSERT INTO dedup_identical_multi_partition SETTINGS async_insert = 1, wait_for_async_insert = 0, async_insert_busy_timeout_min_ms = 600000, async_insert_busy_timeout_max_ms = 600000, async_insert_use_adaptive_busy_timeout = 0, deduplicate_insert = 'enable' VALUES (0, 1), (1, 2);
INSERT INTO dedup_identical_multi_partition SETTINGS async_insert = 1, wait_for_async_insert = 0, async_insert_busy_timeout_min_ms = 600000, async_insert_busy_timeout_max_ms = 600000, async_insert_use_adaptive_busy_timeout = 0, deduplicate_insert = 'enable' VALUES (0, 1), (1, 2);

SYSTEM FLUSH ASYNC INSERT QUEUE dedup_identical_multi_partition;

-- Second flush with the same pair: must fully deduplicate against the deduplication log.
INSERT INTO dedup_identical_multi_partition SETTINGS async_insert = 1, wait_for_async_insert = 0, async_insert_busy_timeout_min_ms = 600000, async_insert_busy_timeout_max_ms = 600000, async_insert_use_adaptive_busy_timeout = 0, deduplicate_insert = 'enable' VALUES (0, 1), (1, 2);
INSERT INTO dedup_identical_multi_partition SETTINGS async_insert = 1, wait_for_async_insert = 0, async_insert_busy_timeout_min_ms = 600000, async_insert_busy_timeout_max_ms = 600000, async_insert_use_adaptive_busy_timeout = 0, deduplicate_insert = 'enable' VALUES (0, 1), (1, 2);

SYSTEM FLUSH ASYNC INSERT QUEUE dedup_identical_multi_partition;

-- Every row must sit in the part of its own partition.
SELECT _partition_id, p, x FROM dedup_identical_multi_partition ORDER BY ALL;
SELECT 'total', count(), 'misplaced', countIf(_partition_id != toString(p)) FROM dedup_identical_multi_partition;

DROP TABLE dedup_identical_multi_partition;
