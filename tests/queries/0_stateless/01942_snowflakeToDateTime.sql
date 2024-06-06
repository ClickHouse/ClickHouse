SET session_timezone = 'UTC'; -- disable timezone randomization
SET allow_experimental_analyzer = 1; -- The old path formats the result with different whitespaces

SELECT '-- negative tests';
SELECT snowflakeToDateTime();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeToDateTime64();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeToDateTime('invalid_snowflake');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime64('invalid_snowflake');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime(123::UInt64, 'invalid_epoch');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime64(123::UInt64, 'invalid_epoch');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime(123::UInt64, materialize(42));  -- {serverError ILLEGAL_COLUMN}
SELECT snowflakeToDateTime64(123::UInt64, materialize(42));  -- {serverError ILLEGAL_COLUMN}
SELECT snowflakeToDateTime(123::UInt64, 42, 42);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime64(123::UInt64, 42, 42);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime(123::UInt64, 42, 'UTC', 'too_many_args');  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeToDateTime64(123::UInt64, 42, 'UTC', 'too_many_args');  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT '-- non-const path';
-- Two const arguments are mapped to two non-const arguments ('getDefaultImplementationForConstants'), the non-const path is taken

-- epoch 0 and 'UTC' are defaults
WITH
    CAST(7204436857747984384 AS Int64) AS i64,
    CAST(7204436857747984384 AS UInt64) AS u64,
    0 AS epoch,
    'UTC' AS tz
SELECT
    tz,
    epoch,
    i64,
    u64,
    snowflakeToDateTime(i64, epoch, tz) as i_dt,
    snowflakeToDateTime(u64, epoch, tz) as u_dt,
    toTypeName(i_dt),
    toTypeName(u_dt),
    snowflakeToDateTime64(i64, epoch, tz) as i_dt64,
    snowflakeToDateTime64(u64, epoch, tz) as u_dt64,
    toTypeName(i_dt64),
    toTypeName(u_dt64)
FORMAT
    Vertical;

-- non-default epoch (Twitter epoch), note the input snowflakeID
WITH
    CAST(1426860704886947840 AS Int64) AS i64,
    CAST(1426860704886947840 AS UInt64) AS u64,
    1288834974657 AS epoch,
    'UTC' AS tz
SELECT
    tz,
    epoch,
    i64,
    u64,
    snowflakeToDateTime(i64, epoch, tz) as i_dt,
    snowflakeToDateTime(u64, epoch, tz) as u_dt,
    toTypeName(i_dt),
    toTypeName(u_dt),
    snowflakeToDateTime64(i64, epoch, tz) as i_dt64,
    snowflakeToDateTime64(u64, epoch, tz) as u_dt64,
    toTypeName(i_dt64),
    toTypeName(u_dt64)
FORMAT
    Vertical;

-- non-default timezone
WITH
    CAST(7204436857747984384 AS Int64) AS i64,
    CAST(7204436857747984384 AS UInt64) AS u64,
    0 AS epoch,
    'Asia/Shanghai' AS tz
SELECT
    tz,
    i64,
    snowflakeToDateTime(i64, epoch, tz) as i_dt,
    snowflakeToDateTime(u64, epoch, tz) as u_dt,
    toTypeName(i_dt),
    toTypeName(u_dt),
    snowflakeToDateTime64(i64, epoch, tz) as i_dt64,
    snowflakeToDateTime64(u64, epoch, tz) as u_dt64,
    toTypeName(i_dt64),
    toTypeName(u_dt64)
FORMAT
    Vertical;

SELECT '-- non-const path';

-- The const path can only be tested by const snowflake + const epoch + non-const time-zone. The latter requires a special setting.
SET allow_nonconst_timezone_arguments = 1;

WITH
    CAST(7204436857747984384 AS Int64) AS i64,
    CAST(7204436857747984384 AS UInt64) AS u64,
    0 as epoch,
    materialize('UTC') AS tz
SELECT
    tz,
    epoch,
    i64,
    u64,
    snowflakeToDateTime(i64, epoch, tz) as i_dt,
    snowflakeToDateTime(u64, epoch, tz) as u_dt,
    toTypeName(i_dt),
    toTypeName(u_dt),
    snowflakeToDateTime64(i64, epoch, tz) as i_dt64,
    snowflakeToDateTime64(u64, epoch, tz) as u_dt64,
    toTypeName(i_dt64),
    toTypeName(u_dt64)
FORMAT
    Vertical;

SELECT '-- Can be combined with generateSnowflakeID';

WITH
    generateSnowflakeID() AS snowflake
SELECT
    snowflakeToDateTime(snowflake),
    snowflakeToDateTime64(snowflake)
FORMAT
    Null;
