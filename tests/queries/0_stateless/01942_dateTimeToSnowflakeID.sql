SET session_timezone = 'UTC'; -- disable timezone randomization
SET enable_analyzer = 1; -- The old path formats the result with different whitespaces

SELECT '-- Negative tests';
SELECT dateTimeToSnowflakeID();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT dateTime64ToSnowflakeID();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT dateTimeToSnowflakeID('invalid_dt');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT dateTime64ToSnowflakeID('invalid_dt');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT dateTimeToSnowflakeID(now(), 'invalid_epoch');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT dateTime64ToSnowflakeID(now64(), 'invalid_epoch');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT dateTimeToSnowflakeID(now(), 42, 'too_many_args');  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT dateTime64ToSnowflakeID(now64(), 42, 'too_many_args');  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT '-- Return type';
SELECT toTypeName(dateTimeToSnowflakeID(now()));
SELECT toTypeName(dateTime64ToSnowflakeID(now64()));

SELECT '-- Standard and twitter epoch';

WITH
    toDateTime('2021-08-15 18:57:56') AS dt,
    toDateTime64('2021-08-15 18:57:56.492', 3) AS dt64,
    1288834974657 AS twitter_epoch
SELECT
    dt,
    dt64,
    dateTimeToSnowflakeID(dt),
    dateTime64ToSnowflakeID(dt64),
    dateTimeToSnowflakeID(dt, twitter_epoch),
    dateTime64ToSnowflakeID(dt64, twitter_epoch)
FORMAT
    Vertical;

SELECT '-- Different DateTime64 scales';

WITH
    toDateTime64('2021-08-15 18:57:56.492', 0, 'UTC') AS dt64_0,
    toDateTime64('2021-08-15 18:57:56.492', 1, 'UTC') AS dt64_1,
    toDateTime64('2021-08-15 18:57:56.492', 2, 'UTC') AS dt64_2,
    toDateTime64('2021-08-15 18:57:56.492', 3, 'UTC') AS dt64_3,
    toDateTime64('2021-08-15 18:57:56.492', 4, 'UTC') AS dt64_4
SELECT
    dateTime64ToSnowflakeID(dt64_0),
    dateTime64ToSnowflakeID(dt64_1),
    dateTime64ToSnowflakeID(dt64_2),
    dateTime64ToSnowflakeID(dt64_3),
    dateTime64ToSnowflakeID(dt64_4)
Format
    Vertical;

SELECT '-- Idempotency';

 -- DateTime64-to-SnowflakeID-to-DateTime64 is idempotent if the scale is <=3 (millisecond precision)
WITH
    now64(0) AS dt64_0,
    now64(1) AS dt64_1,
    now64(2) AS dt64_2,
    now64(3) AS dt64_3
SELECT
    snowflakeIDToDateTime64(dateTime64ToSnowflakeID(dt64_0), 0, 'UTC') == dt64_0,
    snowflakeIDToDateTime64(dateTime64ToSnowflakeID(dt64_1), 0, 'UTC') == dt64_1,
    snowflakeIDToDateTime64(dateTime64ToSnowflakeID(dt64_2), 0, 'UTC') == dt64_2,
    snowflakeIDToDateTime64(dateTime64ToSnowflakeID(dt64_3), 0, 'UTC') == dt64_3
FORMAT
    Vertical;

-- not idempotent
WITH
    toDateTime64('2023-11-11 11:11:11.1231', 4, 'UTC') AS dt64_4
SELECT
    dt64_4,
    snowflakeIDToDateTime64(dateTime64ToSnowflakeID(dt64_4))
FORMAT
    Vertical;
