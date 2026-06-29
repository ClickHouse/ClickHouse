-- Test that finalize_projection_parts_synchronously works correctly:
-- data is written and readable via both the primary key and projections.

DROP TABLE IF EXISTS test_proj_sync;

CREATE TABLE test_proj_sync
(
    key UInt64,
    v1 UInt64,
    v2 UInt64,
    v3 UInt64,
    PROJECTION p1 (SELECT key, v1 ORDER BY v1),
    PROJECTION p2 (SELECT key, v2 ORDER BY v2),
    PROJECTION p3 (SELECT key, v3 ORDER BY v3)
)
ENGINE = MergeTree
ORDER BY key;

-- INSERT with default async finalization
INSERT INTO test_proj_sync SELECT number, number * 10, number * 100, number * 1000 FROM numbers(10000)
SETTINGS finalize_projection_parts_synchronously = 0, max_insert_threads = 1;

-- INSERT with sync finalization
INSERT INTO test_proj_sync SELECT number + 10000, (number + 10000) * 10, (number + 10000) * 100, (number + 10000) * 1000 FROM numbers(10000)
SETTINGS finalize_projection_parts_synchronously = 1, max_insert_threads = 1;

-- Verify total row count
SELECT count() FROM test_proj_sync;

-- Verify data via primary key
SELECT sum(key) FROM test_proj_sync;

-- Verify projections are readable and contain correct data
-- These queries should use the projections (ORDER BY v1/v2/v3)
SELECT sum(v1) FROM test_proj_sync WHERE v1 >= 0;
SELECT sum(v2) FROM test_proj_sync WHERE v2 >= 0;
SELECT sum(v3) FROM test_proj_sync WHERE v3 >= 0;

-- Verify specific rows from the sync-inserted batch
SELECT key, v1, v2, v3 FROM test_proj_sync WHERE key = 15000;

-- Verify no data corruption: aggregates match expected values
-- sum(0..19999) = 19999*20000/2 = 199990000
SELECT sum(key) = 199990000 AS key_sum_ok FROM test_proj_sync;
SELECT sum(v1) = 199990000 * 10 AS v1_sum_ok FROM test_proj_sync;
SELECT sum(v2) = 199990000 * 100 AS v2_sum_ok FROM test_proj_sync;
SELECT sum(v3) = 199990000 * 1000 AS v3_sum_ok FROM test_proj_sync;

DROP TABLE test_proj_sync;
