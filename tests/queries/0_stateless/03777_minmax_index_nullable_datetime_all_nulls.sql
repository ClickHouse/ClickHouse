-- Test for issue #92834: Logical error when querying system.parts 
-- with Nullable DateTime/DateTime64 partition key where all values are NULL
-- Tags: no-parallel

DROP TABLE IF EXISTS test_nullable_datetime_partition;

-- Create a table with a Nullable DateTime column in the partition key
CREATE TABLE test_nullable_datetime_partition
(
    id UInt64,
    event_time Nullable(DateTime)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY id;

-- Insert data where all event_time values are NULL
-- This will create a part where minmax index has POSITIVE_INFINITY (Null type)
INSERT INTO test_nullable_datetime_partition (id, event_time) VALUES (1, NULL), (2, NULL), (3, NULL);

-- This query used to throw: "Logical error: 'Part minmax index by time is neither DateTime or DateTime64'"
-- Now it should return results without error
SELECT count() > 0 as has_parts
FROM system.parts 
WHERE database = currentDatabase() AND table = 'test_nullable_datetime_partition' AND active;

-- Verify min_time and max_time are accessible (should be epoch 0 when all values are NULL)
SELECT min_time = '1970-01-01 00:00:00' AND max_time = '1970-01-01 00:00:00' AS times_are_epoch
FROM system.parts 
WHERE database = currentDatabase() AND table = 'test_nullable_datetime_partition' AND active;

-- Also test with DateTime64
DROP TABLE IF EXISTS test_nullable_datetime64_partition;

CREATE TABLE test_nullable_datetime64_partition
(
    id UInt64,
    event_time Nullable(DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY id;

INSERT INTO test_nullable_datetime64_partition (id, event_time) VALUES (1, NULL), (2, NULL), (3, NULL);

SELECT count() > 0 as has_parts
FROM system.parts 
WHERE database = currentDatabase() AND table = 'test_nullable_datetime64_partition' AND active;

SELECT min_time = '1970-01-01 00:00:00' AND max_time = '1970-01-01 00:00:00' AS times_are_epoch
FROM system.parts 
WHERE database = currentDatabase() AND table = 'test_nullable_datetime64_partition' AND active;

-- Cleanup
DROP TABLE IF EXISTS test_nullable_datetime_partition;
DROP TABLE IF EXISTS test_nullable_datetime64_partition;

