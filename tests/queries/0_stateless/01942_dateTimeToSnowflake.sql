SET session_timezone = 'Africa/Juba';

-- Error cases
SELECT dateTimeToSnowflake();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT dateTime64ToSnowflake();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT dateTimeToSnowflake('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT dateTime64ToSnowflake('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT dateTimeToSnowflake('abc', 123);  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT dateTime64ToSnowflake('abc', 123);  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT '-- const / non-const inputs';

WITH toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS dt
SELECT dt, dateTimeToSnowflake(dt), materialize(dateTimeToSnowflake(dt));

WITH toDateTime64('2021-08-15 18:57:56.492', 3, 'Asia/Shanghai') AS dt64
SELECT dt64, dateTime64ToSnowflake(dt64), materialize(dateTime64ToSnowflake(dt64));

SELECT '-- different DateTime64 scales';

WITH toDateTime64('2021-08-15 18:57:56.492', 0, 'UTC') AS dt64_0,
     toDateTime64('2021-08-15 18:57:56.492', 1, 'UTC') AS dt64_1,
     toDateTime64('2021-08-15 18:57:56.492', 2, 'UTC') AS dt64_2,
     toDateTime64('2021-08-15 18:57:56.492', 3, 'UTC') AS dt64_3,
     toDateTime64('2021-08-15 18:57:56.492', 4, 'UTC') AS dt64_4
SELECT dateTime64ToSnowflake(dt64_0),
       dateTime64ToSnowflake(dt64_1),
       dateTime64ToSnowflake(dt64_2),
       dateTime64ToSnowflake(dt64_3),
       dateTime64ToSnowflake(dt64_4);

-- DateTime64-to-Snowflake-to-DateTime64 is idempotent *if* the scale is <=3 (millisecond precision)
WITH now64(0, 'UTC') AS dt64_0,
     now64(1, 'UTC') AS dt64_1,
     now64(2, 'UTC') AS dt64_2,
     now64(3, 'UTC') AS dt64_3,
     now64(4, 'UTC') AS dt64_4
SELECT snowflakeToDateTime64(dateTime64ToSnowflake(dt64_0)) == dt64_0,
       snowflakeToDateTime64(dateTime64ToSnowflake(dt64_1)) == dt64_1,
       snowflakeToDateTime64(dateTime64ToSnowflake(dt64_2)) == dt64_2,
       snowflakeToDateTime64(dateTime64ToSnowflake(dt64_3)) == dt64_3,
       snowflakeToDateTime64(dateTime64ToSnowflake(dt64_4)) == dt64_4;
