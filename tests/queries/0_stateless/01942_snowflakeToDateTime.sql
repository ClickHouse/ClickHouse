SET session_timezone = 'UTC'; -- disable timezone randomization
-- Negative tests
SELECT snowflakeToDateTime();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeToDateTime64();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeToDateTime('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime64('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime('abc', 123);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime64('abc', 123);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT '-- non-const path';
-- Two const arguments are mapped to two non-const arguments ('getDefaultImplementationForConstants'), the non-const path is taken

WITH
    CAST(1426860704886947840 AS Int64) AS i64,
    CAST(1426860704886947840 AS UInt64) AS u64,
    'UTC' AS tz
SELECT
    tz,
    i64,
    u64,
    snowflakeToDateTime(i64, tz) as i_dt,
    snowflakeToDateTime(u64, tz) as u_dt,
    toTypeName(i_dt),
    toTypeName(u_dt),
    snowflakeToDateTime64(i64, tz) as i_dt64,
    snowflakeToDateTime64(u64, tz) as u_dt64,
    toTypeName(i_dt64),
    toTypeName(u_dt64)
FORMAT
    Vertical;

-- non-default timezone
WITH
    CAST(1426860704886947840 AS Int64) AS i64,
    CAST(1426860704886947840 AS UInt64) AS u64,
    'Asia/Shanghai' AS tz
SELECT
    tz,
    i64,
    snowflakeToDateTime(i64, tz) as i_dt,
    snowflakeToDateTime(u64, tz) as u_dt,
    toTypeName(i_dt),
    toTypeName(u_dt),
    snowflakeToDateTime64(i64, tz) as i_dt64,
    snowflakeToDateTime64(u64, tz) as u_dt64,
    toTypeName(i_dt64),
    toTypeName(u_dt64)
FORMAT
    Vertical;

SELECT '-- non-const path';
-- The const path can only be tested by const snowflake + non-const time-zone. The latter requires a special setting.
SET allow_nonconst_timezone_arguments = 1;

WITH
    CAST(1426860704886947840 AS Int64) AS i64,
    CAST(1426860704886947840 AS UInt64) AS u64,
    materialize('UTC') AS tz
SELECT
    tz,
    i64,
    u64,
    snowflakeToDateTime(i64, tz) as i_dt,
    snowflakeToDateTime(u64, tz) as u_dt,
    toTypeName(i_dt),
    toTypeName(u_dt),
    snowflakeToDateTime64(i64, tz) as i_dt64,
    snowflakeToDateTime64(u64, tz) as u_dt64,
    toTypeName(i_dt64),
    toTypeName(u_dt64)
FORMAT
    Vertical;

SELECT '-- Can be combined with generateSnowflakeID';

WITH
    generateSnowflakeID() AS snowflake
SELECT
    snowflakeToDateTime(snowflake) != now(),
    snowflakeToDateTime64(snowflake) != now()
