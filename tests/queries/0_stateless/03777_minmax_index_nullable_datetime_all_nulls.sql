-- Test for issue #92834: Logical error when querying system.parts 
-- with Nullable DateTime/DateTime64 partition key where all values are NULL
-- Also tests that min_time/max_time are correct after ALTER TABLE changes column order
-- Tags: no-parallel

-- =====================================================
-- Case 1: Exact reproduction from the original issue
-- This is the most reliable way to trigger the bug
-- =====================================================
DROP TABLE IF EXISTS t0;

-- Create table with Enum + DateTime in partition key
CREATE TABLE t0 (c1 Enum('a' = 1) NULL, c2 DateTime) ENGINE = MergeTree() 
    PARTITION BY (c1, c2) ORDER BY tuple() SETTINGS allow_nullable_key = 1;

-- Alter the Enum column to Nullable(Int8) - this changes the type
ALTER TABLE t0 MODIFY COLUMN c1 Nullable(Int8) AFTER c2;

-- Insert a row where c1 has a value but c2 (DateTime) gets default value
-- This creates a part where the minmax index has an unexpected type
INSERT INTO TABLE t0 (c1) VALUES (1);

-- This query triggers the bug: "Part minmax index by time is neither DateTime or DateTime64"
-- After the fix, it should return 1 without error
SELECT 1 FROM system.parts WHERE database = currentDatabase() AND table = 't0' AND active;

DROP TABLE IF EXISTS t0;

-- =====================================================
-- Case 1b: Test that min_time is correct after ALTER (from PR review #93111)
-- After ALTER TABLE MODIFY COLUMN ... AFTER, the cached minmax_idx_time_column_pos
-- was not updated, causing getMinMaxTime() to read from wrong hyperrectangle position
-- =====================================================
DROP TABLE IF EXISTS test_minmax_after_alter;

CREATE TABLE test_minmax_after_alter
(
    c1 Enum('a' = 1) NULL,
    c2 DateTime
)
ENGINE = MergeTree
PARTITION BY (c1, c2)
ORDER BY tuple()
SETTINGS allow_nullable_key = 1;

ALTER TABLE test_minmax_after_alter MODIFY COLUMN c1 Nullable(Int8) AFTER c2;
INSERT INTO test_minmax_after_alter (c1, c2) VALUES (1, toDateTime('2024-01-01 00:00:00'));

-- min_time should match the actual DateTime value (1704067200 or similar depending on timezone)
-- Before the fix, min_time was incorrectly returning 1 (the value of c1) instead of the DateTime
SELECT
    (SELECT min(toUInt32(c2)) FROM test_minmax_after_alter) = toUInt32(min_time) AS min_time_correct
FROM system.parts
WHERE database = currentDatabase() AND table = 'test_minmax_after_alter' AND active;

-- Filtering by min_time should work correctly
-- Before the fix, this returned 0 because min_time was 1 instead of the correct timestamp
SELECT count() > 0 AS filter_works
FROM system.parts
WHERE database = currentDatabase() AND table = 'test_minmax_after_alter' AND active
  AND min_time >= toDateTime('2020-01-01 00:00:00');

DROP TABLE IF EXISTS test_minmax_after_alter;

-- =====================================================
-- Case 2: Direct Nullable DateTime partition key with all NULLs
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime_partition;

CREATE TABLE test_nullable_datetime_partition
(
    id UInt64,
    event_time Nullable(DateTime)
)
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id
SETTINGS allow_nullable_key = 1;

-- Insert data where all event_time values are NULL
INSERT INTO test_nullable_datetime_partition (id, event_time) VALUES (1, NULL), (2, NULL), (3, NULL);

-- Should return 1 (has parts) without error
SELECT 1 FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime_partition' AND active;

DROP TABLE IF EXISTS test_nullable_datetime_partition;

-- =====================================================
-- Case 3: DateTime64 variant  
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime64_partition;

CREATE TABLE test_nullable_datetime64_partition
(
    id UInt64,
    event_time Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_datetime64_partition (id, event_time) VALUES (1, NULL), (2, NULL), (3, NULL);

-- Should return 1 (has parts) without error
SELECT 1 FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime64_partition' AND active;

DROP TABLE IF EXISTS test_nullable_datetime64_partition;
