-- Test for issue #92834: Logical error when querying system.parts 
-- with Nullable DateTime/DateTime64 partition key where all values are NULL
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
-- Case 2: Direct Nullable DateTime partition key with all NULLs
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime_partition;

CREATE TABLE test_nullable_datetime_partition
(
    id UInt64,
    event_time Nullable(DateTime)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
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
PARTITION BY toYYYYMM(event_time)
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_datetime64_partition (id, event_time) VALUES (1, NULL), (2, NULL), (3, NULL);

-- Should return 1 (has parts) without error
SELECT 1 FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime64_partition' AND active;

DROP TABLE IF EXISTS test_nullable_datetime64_partition;
