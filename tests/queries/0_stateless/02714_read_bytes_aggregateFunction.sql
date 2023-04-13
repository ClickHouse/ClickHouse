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

SELECT 'AggregateFunction(argMax, String, DateTime)',
       read_rows,
       read_bytes
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    query = 'SELECT argMaxMerge(amax) FROM test FORMAT Null;' AND
    type = 2 AND event_date >= yesterday()
ORDER BY event_time_microseconds;
