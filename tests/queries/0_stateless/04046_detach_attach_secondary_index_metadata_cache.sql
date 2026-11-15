-- Tags: no-parallel-replicas
-- Coverage gap for PR #99529 (fix incorrect part marking as broken after DETACH/ATTACH).
-- The PR's own test (04041) uses tables without secondary indices, so
-- removeIndexMarksFromCache returns early at secondary_indices.empty() and the
-- per-index mark eviction loop never runs.
-- This test uses tables WITH secondary indices (bloom_filter, minmax) so the loop
-- actually executes and mark cache entries are removed. It also covers
-- PARTITION BY + secondary index.

DROP TABLE IF EXISTS t_04046_a;
DROP TABLE IF EXISTS t_04046_b;

SET enable_shared_storage_snapshot_in_query = 1;

CREATE TABLE t_04046_a
(
    key UInt64,
    val String,
    INDEX idx_val val TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS prewarm_mark_cache = 1;

CREATE TABLE t_04046_b
(
    key UInt64,
    val UInt64,
    INDEX idx_val val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY key
PARTITION BY key % 2
SETTINGS prewarm_mark_cache = 1;

INSERT INTO t_04046_a SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_04046_b SELECT number, number FROM numbers(100);

DETACH TABLE t_04046_a;
DETACH TABLE t_04046_b;
ATTACH TABLE t_04046_a;

SELECT count() FROM t_04046_a;
SELECT sum(key) FROM t_04046_a;

ATTACH TABLE t_04046_b;

SELECT count() FROM t_04046_b;
SELECT sum(val) FROM t_04046_b;

DROP TABLE t_04046_a;
DROP TABLE t_04046_b;

-- Repeat with enable_shared_storage_snapshot_in_query = 0 as a sanity check
SET enable_shared_storage_snapshot_in_query = 0;

CREATE TABLE t_04046_a
(
    key UInt64,
    val String,
    INDEX idx_val val TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS prewarm_mark_cache = 1;

CREATE TABLE t_04046_b
(
    key UInt64,
    val UInt64,
    INDEX idx_val val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY key
PARTITION BY key % 2
SETTINGS prewarm_mark_cache = 1;

INSERT INTO t_04046_a SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_04046_b SELECT number, number FROM numbers(100);

DETACH TABLE t_04046_a;
DETACH TABLE t_04046_b;
ATTACH TABLE t_04046_a;

SELECT count() FROM t_04046_a;
SELECT sum(key) FROM t_04046_a;

ATTACH TABLE t_04046_b;

SELECT count() FROM t_04046_b;
SELECT sum(val) FROM t_04046_b;

DROP TABLE t_04046_a;
DROP TABLE t_04046_b;
