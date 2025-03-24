SET session_timezone = 'UTC'; -- disable timezone randomization
SET enable_analyzer = 1; -- The old path formats the result with different whitespaces
SET output_format_pretty_single_large_number_tip_threshold = 0;

SELECT '-- Negative tests';
SELECT snowflakeIDToDateTime();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeIDToDateTime64();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeIDToDateTime('invalid_snowflake');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeIDToDateTime64('invalid_snowflake');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeIDToDateTime(123::UInt64, 'invalid_epoch');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeIDToDateTime64(123::UInt64, 'invalid_epoch');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeIDToDateTime(123::UInt64, materialize(42));  -- {serverError ILLEGAL_COLUMN}
SELECT snowflakeIDToDateTime64(123::UInt64, materialize(42));  -- {serverError ILLEGAL_COLUMN}
SELECT snowflakeIDToDateTime(123::UInt64, 42, 42);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeIDToDateTime64(123::UInt64, 42, 42);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeIDToDateTime(123::UInt64, 42, 'UTC', 'too_many_args');  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeIDToDateTime64(123::UInt64, 42, 'UTC', 'too_many_args');  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT '-- Return type';
SELECT toTypeName(snowflakeIDToDateTime(123::UInt64));
SELECT toTypeName(snowflakeIDToDateTime64(123::UInt64));

SELECT '-- Non-const path';
-- Two const arguments are mapped to two non-const arguments ('getDefaultImplementationForConstants'), the non-const path is taken

WITH
    7204436857747984384 AS sf
SELECT
    sf,
    snowflakeIDToDateTime(sf) as dt,
    snowflakeIDToDateTime64(sf) as dt64
FORMAT
    Vertical;

-- With Twitter Snowflake ID and Twitter epoch
WITH
    1426981498778550272 AS sf,
    1288834974657 AS epoch
SELECT
    sf,
    snowflakeIDToDateTime(sf, epoch) as dt,
    snowflakeIDToDateTime64(sf, epoch) as dt64
FORMAT
    Vertical;

-- non-default timezone
WITH
    7204436857747984384 AS sf,
    0 AS epoch, -- default epoch
    'Asia/Shanghai' AS tz
SELECT
    sf,
    snowflakeIDToDateTime(sf, epoch, tz) as dt,
    snowflakeIDToDateTime64(sf, epoch, tz) as dt64
FORMAT
    Vertical;

SELECT '-- Const path';

-- The const path can only be tested by const snowflake + const epoch + non-const time-zone. The latter requires a special setting.
WITH
    7204436857747984384 AS sf,
    0 AS epoch, -- default epoch
    materialize('Asia/Shanghai') AS tz
SELECT
    sf,
    snowflakeIDToDateTime(sf, epoch, tz) as dt,
    snowflakeIDToDateTime64(sf, epoch, tz) as dt64
FORMAT
    Vertical
SETTINGS
    allow_nonconst_timezone_arguments = 1;


SELECT '-- Can be combined with generateSnowflakeID';

WITH
    generateSnowflakeID() AS snowflake
SELECT
    snowflakeIDToDateTime(snowflake),
    snowflakeIDToDateTime64(snowflake)
FORMAT
    Null;
