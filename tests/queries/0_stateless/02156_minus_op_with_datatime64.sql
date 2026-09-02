-- Test subtraction without materialize()
SELECT
-- DateTime64 (scale 3) - DateTime
toDateTime64('2023-10-01 12:00:00', 3) - toDateTime('2023-10-01 11:00:00') AS result_no_materialize_1,
toDateTime64('2023-10-01 12:00:00.123', 3) - toDateTime('2023-10-01 11:00:00') AS result_no_materialize_2,

-- DateTime64 (scale 0) - DateTime
toDateTime64('2023-10-02 12:00:00', 0) - toDateTime('2023-10-01 11:00:00') AS result_no_materialize_3,

-- DateTime64 (scale 3) - DateTime64 (scale 6)
toDateTime64('2023-10-01 12:00:00.123', 3) - toDateTime64('2023-10-01 11:00:00.123456', 6) AS result_no_materialize_4,

-- DateTime64 (scale 6) - DateTime64 (scale 3)
toDateTime64('2023-10-01 12:00:00.123456', 6) - toDateTime64('2023-10-01 11:00:00.123', 3) AS result_no_materialize_5,

-- DateTime - DateTime64 (scale 3)
toDateTime('2023-10-01 12:00:00') - toDateTime64('2023-10-01 11:00:00', 3) AS result_no_materialize_6,

-- DateTime - DateTime64 (scale 6)
toDateTime('2023-10-01 12:00:00') - toDateTime64('2023-10-01 11:00:00', 6) AS result_no_materialize_7;

-- Test subtraction with materialize() on left side
SELECT
materialize(toDateTime64('2023-10-01 12:00:00', 3)) - toDateTime('2023-10-01 11:00:00') AS result_left_materialize_1,
materialize(toDateTime64('2023-10-01 12:00:00.123', 3)) - toDateTime('2023-10-01 11:00:00') AS result_left_materialize_2,

materialize(toDateTime64('2023-10-02 12:00:00', 0)) - toDateTime('2023-10-01 11:00:00') AS result_left_materialize_3,

materialize(toDateTime64('2023-10-01 12:00:00.123', 3)) - toDateTime64('2023-10-01 11:00:00.123456', 6) AS result_left_materialize_4,

materialize(toDateTime64('2023-10-01 12:00:00.123456', 6)) - toDateTime64('2023-10-01 11:00:00.123', 3) AS result_left_materialize_5,

materialize(toDateTime('2023-10-01 12:00:00')) - toDateTime64('2023-10-01 11:00:00', 3) AS result_left_materialize_6,

materialize(toDateTime('2023-10-01 12:00:00')) - toDateTime64('2023-10-01 11:00:00', 6) AS result_left_materialize_7;

-- Test subtraction with materialize() on right side
SELECT
toDateTime64('2023-10-01 12:00:00', 3) - materialize(toDateTime('2023-10-01 11:00:00')) AS result_right_materialize_1,
toDateTime64('2023-10-01 12:00:00.123', 3) - materialize(toDateTime('2023-10-01 11:00:00')) AS result_right_materialize_2,

toDateTime64('2023-10-01 12:00:00', 0) - materialize(toDateTime('2023-10-01 11:00:00')) AS result_right_materialize_3,

toDateTime('2023-10-01 12:00:00') - materialize(toDateTime64('2023-10-01 11:00:00', 3)) AS result_right_materialize_4;

-- Test subtraction with materialize() on both sides
SELECT
materialize(toDateTime64('2023-10-01 12:00:00', 3)) - materialize(toDateTime('2023-10-01 11:00:00')) AS result_both_materialize_1,
materialize(toDateTime64('2023-10-01 12:00:00.123', 3)) - materialize(toDateTime('2023-10-01 11:00:00')) AS result_both_materialize_2,

materialize(toDateTime64('2023-10-01 12:00:00', 0)) - materialize(toDateTime('2023-10-01 11:00:00')) AS result_both_materialize_3,

materialize(toDateTime('2023-10-01 12:00:00')) - materialize(toDateTime64('2023-10-01 11:00:00', 3)) AS result_both_materialize_4;

-- Test overflow
SELECT
materialize(toDateTime64('2262-04-11 23:47:16', 9, 'UTC')) - toDateTime64('1900-01-01 00:00:00', 9, 'UTC') FORMAT Null; -- { serverError DECIMAL_OVERFLOW }

SELECT
materialize(toDateTime64('1900-01-01 00:00:00', 0, 'UTC')) - materialize(toDateTime64('2262-04-11 23:47:16', 9, 'UTC')); -- { serverError DECIMAL_OVERFLOW }

SELECT
materialize(toDateTime64('2262-04-11 23:47:16', 9, 'UTC')) - toDateTime64('1900-01-01 00:00:00', 9, 'UTC'),
materialize(toDateTime64('1900-01-01 00:00:00', 0, 'UTC')) - materialize(toDateTime64('2262-04-11 23:47:16', 9, 'UTC'))
SETTINGS decimal_check_overflow=0 FORMAT Null;
