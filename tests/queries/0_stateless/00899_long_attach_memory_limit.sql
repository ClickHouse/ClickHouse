-- Tags: long, no-debug, no-parallel, no-fasttest, no-msan, no-tsan, no-flaky-check
-- Random settings limits: optimize_trivial_count_query=(1, None)
-- This test is slow under MSan or TSan.
-- no-flaky-check: the ThreadFuzzer of the flaky check makes the 5M-row INSERT over 10x slower,
-- which runs past the per-test timeout.

DROP TABLE IF EXISTS index_memory;
CREATE TABLE index_memory (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO index_memory SELECT * FROM system.numbers LIMIT 5000000;
SELECT count() FROM index_memory;
DETACH TABLE index_memory;
SET max_memory_usage = 39000000;
ATTACH TABLE index_memory;
SELECT count() FROM index_memory;
DROP TABLE index_memory;
