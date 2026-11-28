SET query_plan_convert_distinct_to_aggregation = 1;
SET enable_adaptive_aggregator = 0;
SET distinct_overflow_mode = 'throw';
SET max_threads = 2;
SET max_block_size = 25;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
SET group_by_two_level_threshold_bytes = 0;
SET enable_packed_string_keys_in_aggregation = 1;

-- Each branch fits within the row bound, but merging their disjoint key sets exceeds it.
SELECT toUInt8(number) AS k FROM numbers(100) UNION DISTINCT SELECT toUInt8(number + 100) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT toUInt8(number) AS k FROM numbers(100) UNION DISTINCT SELECT toUInt8(number + 100) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 1, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT toUInt16(number) AS k FROM numbers(100) UNION DISTINCT SELECT toUInt16(number + 100) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT toUInt16(number) AS k FROM numbers(100) UNION DISTINCT SELECT toUInt16(number + 100) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 1, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT number AS k FROM numbers(100) UNION DISTINCT SELECT number + 100 AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT number AS k FROM numbers(100) UNION DISTINCT SELECT number + 100 AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 1, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT toString(number) AS k FROM numbers(100) UNION DISTINCT SELECT toString(number + 100) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT toString(number) AS k FROM numbers(100) UNION DISTINCT SELECT toString(number + 100) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 1, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT toNullable(number) AS k FROM numbers(100) UNION DISTINCT SELECT toNullable(number + 100) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT toNullable(number) AS k FROM numbers(100) UNION DISTINCT SELECT toNullable(number + 100) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 1, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT tuple(number, toString(number)) AS k FROM numbers(100) UNION DISTINCT SELECT tuple(number + 100, toString(number + 100)) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT tuple(number, toString(number)) AS k FROM numbers(100) UNION DISTINCT SELECT tuple(number + 100, toString(number + 100)) AS k FROM numbers(100)
SETTINGS max_rows_in_distinct = 150, group_by_two_level_threshold = 1, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT number AS k FROM numbers(200000) UNION DISTINCT SELECT number + 200000 AS k FROM numbers(200000)
SETTINGS max_rows_in_distinct = 300000, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT number AS k FROM numbers(200000) UNION DISTINCT SELECT number + 200000 AS k FROM numbers(200000)
SETTINGS max_rows_in_distinct = 300000, group_by_two_level_threshold = 1, enable_parallel_single_level_merge = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Duplicate keys from different streams count once in the merged set.
SELECT count() FROM (SELECT number FROM numbers(100) UNION DISTINCT SELECT number FROM numbers(100))
SETTINGS max_rows_in_distinct = 100, group_by_two_level_threshold = 0;
SELECT count() FROM (SELECT number FROM numbers(100) UNION DISTINCT SELECT number FROM numbers(100))
SETTINGS max_rows_in_distinct = 100, group_by_two_level_threshold = 1;

-- Individual string branches fit the byte bound, while their combined state exceeds it.
SELECT count() FROM (SELECT DISTINCT concat(repeat('x', 60), toString(number)) FROM numbers(1000))
SETTINGS max_bytes_in_distinct = 262144, group_by_two_level_threshold = 0;
SELECT count() FROM (SELECT DISTINCT concat(repeat('y', 60), toString(number)) FROM numbers(1000))
SETTINGS max_bytes_in_distinct = 262144, group_by_two_level_threshold = 0;
SELECT concat(repeat('x', 60), toString(number)) AS k FROM numbers(1000)
UNION DISTINCT SELECT concat(repeat('y', 60), toString(number)) AS k FROM numbers(1000)
SETTINGS max_bytes_in_distinct = 262144, group_by_two_level_threshold = 0 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
-- Two-level hash tables enforce the byte bound during construction.
SELECT concat(repeat('x', 60), toString(number)) AS k FROM numbers(1000)
UNION DISTINCT SELECT concat(repeat('y', 60), toString(number)) AS k FROM numbers(1000)
SETTINGS max_bytes_in_distinct = 262144, group_by_two_level_threshold = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- A generous byte bound allows every merge path to finish, including parallel partition merging.
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(1000000))
SETTINGS max_bytes_in_distinct = 100000000, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 0;
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(1000000))
SETTINGS max_bytes_in_distinct = 100000000, group_by_two_level_threshold = 0, enable_parallel_single_level_merge = 1;
SELECT count(), sum(number) FROM (SELECT DISTINCT number FROM numbers_mt(1000000))
SETTINGS max_bytes_in_distinct = 100000000, group_by_two_level_threshold = 1;
