-- Async insert deduplication must work correctly when an INSERT spans multiple partitions.
-- The deduplication hash for each token is computed once (via prewarmDataHashes) and then
-- inherited by per-partition clones, so this verifies both correctness and that the optimisation
-- introduces no hash-collision false positives or negatives. The batched scenario below queues
-- several async inserts into one flush so the sink builds multi-token per-partition
-- DeduplicationInfo copies (the prewarmDataHashes -> filterToPartition -> filterImpl hot path).

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;

CREATE TABLE t (key Int64, value String)
ENGINE = MergeTree
PARTITION BY key % 4
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;

SELECT 'single-flush';

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

SELECT 'batched-flush';

CREATE TABLE t2 (key Int64, value String)
ENGINE = MergeTree
PARTITION BY key % 4
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;

-- Queue several multi-partition async inserts into ONE flush (wait_for_async_insert = 0 + adaptive
-- timeout off) so the sink sees multiple tokens in one consume and filterToPartition must split them.
SET async_insert = 1, wait_for_async_insert = 0, async_insert_deduplicate = 1;
SET async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_min_ms = 5000, async_insert_busy_timeout_max_ms = 600000;

INSERT INTO t2 VALUES (0,'A'),(1,'B'),(2,'C'),(3,'D');
INSERT INTO t2 VALUES (0,'A'),(1,'B'),(2,'C'),(3,'D');  -- identical -> fully deduplicated
INSERT INTO t2 VALUES (0,'E'),(1,'F'),(2,'G'),(3,'H');  -- distinct -> survives

SYSTEM FLUSH ASYNC INSERT QUEUE t2;

-- Regardless of how the entries coalesce, the deduplicated result is 8 rows.
SELECT count() FROM t2;
SELECT * FROM t2 ORDER BY key, value;

DROP TABLE t;
DROP TABLE t2;
