-- Final hash partitions share size limits and stop all inputs when a `BREAK` limit is reached.
SET max_threads = 4;
SET max_block_size = 1000;
SET allow_parallel_distinct = 1;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
SET optimize_distinct_in_order = 1;
SET max_execution_time = 10;

-- Each preliminary set holds one key below its limit, then suppresses every remaining chunk.
SELECT count() BETWEEN 1 AND 2
FROM
(
    SELECT materialize(toUInt64(0)) AS k FROM system.numbers_mt
    UNION DISTINCT
    SELECT materialize(toUInt64(1)) AS k FROM system.numbers_mt
)
SETTINGS max_rows_in_distinct = 2, distinct_overflow_mode = 'break';

-- Each preliminary set holds one key below its limit, then suppresses every remaining chunk.
SELECT count() BETWEEN 1 AND 2
FROM
(
    SELECT materialize(toUInt64(0)) AS k FROM system.numbers_mt
    UNION DISTINCT
    SELECT materialize(toUInt64(1)) AS k FROM system.numbers_mt
)
SETTINGS max_bytes_in_distinct = 4096, distinct_overflow_mode = 'break';

-- Each local set fits, but the combined set exceeds the global limit.
SELECT materialize(toUInt64(0)) AS k FROM numbers_mt(10000)
UNION DISTINCT
SELECT materialize(toUInt64(1)) AS k FROM numbers_mt(10000)
SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'throw' FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Each local set fits, but the combined set exceeds the global limit.
SELECT materialize(toUInt64(0)) AS k FROM numbers_mt(10000)
UNION DISTINCT
SELECT materialize(toUInt64(1)) AS k FROM numbers_mt(10000)
SETTINGS max_bytes_in_distinct = 4095, distinct_overflow_mode = 'throw' FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- A preliminary `BREAK` limit may stop after the first chunk of an unbounded input.
SELECT count() >= 1
FROM (SELECT DISTINCT number % 2 FROM system.numbers_mt)
SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'break';

-- An abandoning preliminary `DISTINCT` does not lift the byte limit: the keys retained by the final
-- `DISTINCT` are still checked, in the single-stream plan and in the hash-scattered one alike, where
-- the check sums the retained bytes of every stream.
SELECT DISTINCT concat(toString(number), repeat('x', 10000)) AS s
FROM numbers_mt(2000)
ORDER BY s
SETTINGS allow_preliminary_distinct_abandoning = 1, allow_parallel_distinct = 0,
    max_block_size = 5000, max_bytes_in_distinct = 1048576 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT DISTINCT concat(toString(number), repeat('x', 10000)) AS s
FROM numbers_mt(2000)
ORDER BY s
SETTINGS allow_preliminary_distinct_abandoning = 1, allow_parallel_distinct = 1,
    max_block_size = 5000, max_bytes_in_distinct = 1048576 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
