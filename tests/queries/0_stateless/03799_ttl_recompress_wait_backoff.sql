-- Tags: no-replicated-database, no-parallel
-- This test verifies that TTL recompression wait has proper backoff

DROP TABLE IF EXISTS ttl_recompress_test_1 SYNC;
DROP TABLE IF EXISTS ttl_recompress_test_2 SYNC;

-- Create two replicas with wait backoff enabled (default)
CREATE TABLE ttl_recompress_test_1 (
    id UInt64,
    data String,
    event_time DateTime
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/ttl_recompress_test', '1')
ORDER BY id
TTL event_time + INTERVAL 1 SECOND RECOMPRESS CODEC(ZSTD(10))
SETTINGS
    try_fetch_recompressed_part_timeout = 60,
    merge_with_ttl_timeout = 1,
    max_postpone_time_for_waiting_ms = 5000;

CREATE TABLE ttl_recompress_test_2 (
    id UInt64,
    data String,
    event_time DateTime
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/ttl_recompress_test', '2')
ORDER BY id
TTL event_time + INTERVAL 1 SECOND RECOMPRESS CODEC(ZSTD(10))
SETTINGS
    try_fetch_recompressed_part_timeout = 60,
    merge_with_ttl_timeout = 1,
    max_postpone_time_for_waiting_ms = 5000;

-- Insert data that will trigger TTL
INSERT INTO ttl_recompress_test_1 VALUES (1, 'test', now() - INTERVAL 2 SECOND);

SYSTEM SYNC REPLICA ttl_recompress_test_2;

-- Wait for TTL merge to be scheduled and some postpones to happen
SELECT sleepEachRow(3) FROM numbers(1) FORMAT Null;

-- Check that we don't have excessive postpone count for TTL recompress entries
-- With backoff (5 second max), in 3 seconds we expect roughly:
--   1ms + 2ms + 4ms + 8ms + 16ms + ... converges quickly
-- Without backoff, num_postponed could be thousands
-- With backoff, should be < 50 in 3 seconds
SELECT
    if(count() > 0 AND max(num_postponed) < 50, 'OK', 'FAIL') AS status
FROM system.replication_queue
WHERE database = currentDatabase()
    AND table LIKE 'ttl_recompress_test%'
    AND type = 'MERGE_PARTS'
    AND postpone_reason LIKE '%TTL recompression%';

-- Cleanup
DROP TABLE IF EXISTS ttl_recompress_test_1 SYNC;
DROP TABLE IF EXISTS ttl_recompress_test_2 SYNC;
