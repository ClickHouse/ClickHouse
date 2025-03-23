SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

CREATE TABLE test (id UInt64, `amax` AggregateFunction(argMax, String, DateTime))
ENGINE=MergeTree()
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization=1 -- Sparse columns will take more bytes for a single row
AS
    SELECT number, argMaxState(number::String, '2023-04-12 16:23:01'::DateTime)
    FROM numbers(1)
    GROUP BY number;

SELECT sum(id) FROM test FORMAT Null;
SELECT argMaxMerge(amax) FROM test FORMAT Null;

INSERT INTO test
    SELECT number, argMaxState(number::String, '2023-04-12 16:23:01'::DateTime)
    FROM numbers(9)
    GROUP BY number;

SELECT sum(id) FROM test FORMAT Null;
SELECT argMaxMerge(amax) FROM test FORMAT Null;

INSERT INTO test
SELECT number, argMaxState(number::String, '2023-04-12 16:23:01'::DateTime)
FROM numbers(990)
GROUP BY number;

SELECT sum(id) FROM test FORMAT Null;
SELECT argMaxMerge(amax) FROM test FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT 'UInt64',
       read_rows,
       read_bytes
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    query = 'SELECT sum(id) FROM test FORMAT Null;' AND
    type = 2 AND event_date >= yesterday()
ORDER BY event_time_microseconds;

-- Size of ColumnAggregateFunction: Number of pointers * pointer size + arena size
-- 1 * 8 + AggregateFunction(argMax, String, DateTime)
--
-- Size of AggregateFunction(argMax, String, DateTime):
-- 1 Base class + 1 specific/value class:
-- Base class: MAX(sizeOf(SingleValueDataFixed<T>), sizeOf(SingleValueDataString), sizeOf(SingleValueDataGeneric)) = 64
-- Specific class: SingleValueDataFixed(DateTime) = 4 + 1. With padding = 8
-- Total: 8 + 64 + 8 = 80
--
-- ColumnAggregateFunction total: 8 + 2 * 64 = 136
SELECT 'AggregateFunction(argMax, String, DateTime)',
       read_rows,
       read_bytes
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    query = 'SELECT argMaxMerge(amax) FROM test FORMAT Null;' AND
    type = 2 AND event_date >= yesterday()
ORDER BY event_time_microseconds;
