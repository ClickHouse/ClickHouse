-- Tags: long, no-debug, no-parallel, no-fasttest, no-msan, no-tsan
-- Tag no-parallel: ATTACH must load 5M-row MergeTree metadata within a tight 39MB per-query memory budget; concurrent-suite memory pressure risks tipping actual allocations over that budget and causing a false failure
-- This test is slow under MSan or TSan.

DROP TABLE IF EXISTS index_memory;
CREATE TABLE index_memory (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO index_memory SELECT * FROM system.numbers LIMIT 5000000;
SELECT count() FROM index_memory;
DETACH TABLE index_memory;
SET max_memory_usage = 39000000;
ATTACH TABLE index_memory;
SELECT count() FROM index_memory;
DROP TABLE index_memory;
