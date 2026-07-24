-- Async insert deduplication must work correctly when an INSERT spans multiple partitions.
-- The deduplication hash for each token is computed once (via prewarmDataHashes) and then
-- inherited by per-partition clones, so this test verifies both correctness and that the
-- optimisation does not introduce any hash-collision false positives or false negatives.

DROP TABLE IF EXISTS t;

CREATE TABLE t (key Int64, value String)
ENGINE = MergeTree
PARTITION BY key % 4
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;

-- First insert: 4 rows spread across 4 partitions.
INSERT INTO t SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1
VALUES (0,'A'),(1,'B'),(2,'C'),(3,'D');

SELECT count() FROM t;

-- Identical insert: must be fully deduplicated, count stays at 4.
INSERT INTO t SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1
VALUES (0,'A'),(1,'B'),(2,'C'),(3,'D');

SELECT count() FROM t;

-- Different data: must NOT be deduplicated, count reaches 8.
INSERT INTO t SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1
VALUES (0,'E'),(1,'F'),(2,'G'),(3,'H');

SELECT count() FROM t;

SELECT * FROM t ORDER BY key, value;

DROP TABLE t;
