-- Tags: distributed

-- Test that negative LIMIT works correctly with distributed queries.
-- This test verifies the fix for the pipeline stuck issue that occurred when
-- negative limits were incorrectly applied as preliminary limits on shards.
-- Negative limits need to see all data to determine which rows to return.

DROP TABLE IF EXISTS t_negative_limit_dist;
CREATE TABLE t_negative_limit_dist (id UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_negative_limit_dist SELECT number FROM numbers(100);

-- Test 1: Simple negative limit with remote() should work without pipeline stuck
SELECT id FROM remote('127.0.0.1', currentDatabase(), t_negative_limit_dist) ORDER BY id LIMIT -5;

-- Test 2: Negative limit in subquery with ORDER BY outside
SELECT * FROM (SELECT id FROM remote('127.0.0.1', currentDatabase(), t_negative_limit_dist) LIMIT -10) ORDER BY id;

DROP TABLE t_negative_limit_dist;
