SET allow_deprecated_snowflake_conversion_functions = 1; -- Force-enable deprecated snowflake conversion functions (in case this is randomized in CI)

-- Error cases
SELECT snowflakeToDateTime();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT snowflakeToDateTime64();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT snowflakeToDateTime('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime64('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT snowflakeToDateTime('abc', 123);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT snowflakeToDateTime64('abc', 123);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT snowflakeToDateTime(123::Int64) SETTINGS allow_deprecated_snowflake_conversion_functions = 0; -- { serverError DEPRECATED_FUNCTION }
SELECT snowflakeToDateTime64(123::Int64) SETTINGS allow_deprecated_snowflake_conversion_functions = 0; -- { serverError DEPRECATED_FUNCTION }

SELECT 'const column';
WITH
    CAST(1426860704886947840 AS Int64) AS i64,
    'UTC' AS tz
SELECT
    tz,
    i64,
    snowflakeToDateTime(i64, tz) as dt,
    toTypeName(dt),
    snowflakeToDateTime64(i64, tz) as dt64,
    toTypeName(dt64);

WITH
    CAST(1426860704886947840 AS Int64) AS i64,
    'Asia/Shanghai' AS tz
SELECT
    tz,
    i64,
    snowflakeToDateTime(i64, tz) as dt,
    toTypeName(dt),
    snowflakeToDateTime64(i64, tz) as dt64,
    toTypeName(dt64);


DROP TABLE IF EXISTS tab;
CREATE TABLE tab(val Int64, tz String) engine = Log;
INSERT INTO tab VALUES (42, 'Asia/Singapore');

SELECT 1 FROM tab WHERE snowflakeToDateTime(42::Int64, tz) != now() SETTINGS allow_nonconst_timezone_arguments = 1;
SELECT 1 FROM tab WHERE snowflakeToDateTime64(42::Int64, tz) != now() SETTINGS allow_nonconst_timezone_arguments = 1;

DROP TABLE tab;
