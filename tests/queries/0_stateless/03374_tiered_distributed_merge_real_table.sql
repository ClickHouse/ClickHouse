-- Test TieredDistributedMerge engine with real table connection
-- This test ensures we can actually create tables and select data

DROP TABLE IF EXISTS test_local_table;
DROP TABLE IF EXISTS test_tiered_real_connection;

-- Create a local table for testing
CREATE TABLE test_local_table
(
    `id` UInt32,
    `name` String,
    `event_time` DateTime,
    `value` Float64
) ENGINE = MergeTree()
ORDER BY id;

-- Insert some test data
INSERT INTO test_local_table VALUES
    (1, 'Alice', '2022-01-01 10:00:00', 100.5),
    (2, 'Bob', '2022-01-02 11:00:00', 200.3),
    (3, 'Charlie', '2022-01-03 12:00:00', 150.7),
    (4, 'David', '2022-01-04 13:00:00', 300.2),
    (5, 'Eve', '2022-01-05 14:00:00', 250.1);

-- Create TieredDistributedMerge table that connects to localhost (current server)
-- This will create a real connection to the local table
CREATE TABLE test_tiered_real_connection
(
    `id` UInt32,
    `name` String,
    `event_time` DateTime,
    `value` Float64
) ENGINE = TieredDistributedMerge(
    remote('127.0.0.1:9000,127.0.0.2:9000', currentDatabase(), 'test_local_table'),
    id > 0
);

-- Test that we can select data from the TieredDistributedMerge table
-- This should return the same data as the local table
SELECT count() FROM test_tiered_real_connection;

-- Test with WHERE condition
SELECT count() FROM test_tiered_real_connection WHERE value > 200;

-- Test with ORDER BY
SELECT id, name, value FROM test_tiered_real_connection WHERE id > 2 ORDER BY value DESC;

-- Test with LIMIT
SELECT * FROM test_tiered_real_connection ORDER BY id LIMIT 3;

SET prefer_localhost_replica = 1; -- avoid getting different plans due to that setting

-- Test EXPLAIN to see the query plan
EXPLAIN SELECT * FROM test_tiered_real_connection WHERE value > 150;

-- Test EXPLAIN with more complex query
EXPLAIN SELECT 
    name,
    count() as count,
    avg(value) as avg_value
FROM test_tiered_real_connection 
WHERE event_time >= '2022-01-02'
GROUP BY name
ORDER BY avg_value DESC;

-- Clean up
DROP TABLE IF EXISTS test_tiered_real_connection;
DROP TABLE IF EXISTS test_local_table;
