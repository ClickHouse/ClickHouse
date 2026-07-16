SET use_legacy_to_time=0;
-- 1. Test toTime with various types of inputs and timezone parameter
SELECT toTime('12:34:56', 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime(toDateTime('2023-01-01 12:34:56'), 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime(toDateTime64('2023-01-01 12:34:56.789', 3), 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime(45296, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime(now(), 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 2. Test toTime64 with various types of inputs and timezone parameter
SELECT toTime64('12:34:56.789', 3, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64(toDateTime('2023-01-01 12:34:56'), 3, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64(toDateTime64('2023-01-01 12:34:56.789', 3), 3, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64(45296789, 3, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64(now(), 3, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 3. Test OrNull variants with timezone parameter
SELECT toTimeOrNull('12:34:56', 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64OrNull('12:34:56.789', 3, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 4. Test OrZero variants with timezone parameter
SELECT toTimeOrZero('12:34:56', 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64OrZero('12:34:56.789', 3, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 5. Test array inputs with timezone parameter
SELECT toTime(['12:34:56', '01:23:45'], 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64(['12:34:56.789', '01:23:45.678'], 3, 'UTC'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 6. Test with different timezone formats to ensure all are rejected
SELECT toTime('12:34:56', 'Europe/Moscow'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64('12:34:56.789', 3, 'Europe/Moscow'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime('12:34:56', '+03:00'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toTime64('12:34:56.789', 3, '+03:00'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 7. Test for a table creation where columns are Time[64] with timezone
DROP TABLE IF EXISTS test_time;
DROP TABLE IF EXISTS test_time64;
CREATE TABLE test_time (t Time('UTC')) engine=MergeTree; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE test_time64 (t Time64(3, 'UTC')) engine=MergeTree; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Clean up
DROP TABLE IF EXISTS test_time;
DROP TABLE IF EXISTS test_time64;
