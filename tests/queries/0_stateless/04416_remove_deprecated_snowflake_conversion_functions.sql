-- Tests that the deprecated Snowflake conversion functions `snowflakeToDateTime`, `snowflakeToDateTime64`,
-- `dateTimeToSnowflake`, and `dateTime64ToSnowflake` (deprecated in v24.6) are removed, and that the now-obsolete
-- setting `allow_deprecated_snowflake_conversion_functions` can no longer re-enable them.

-- The setting is now obsolete: it is accepted for compatibility but has no effect.
SET allow_deprecated_snowflake_conversion_functions = 1;

-- Even with the obsolete setting enabled, the removed functions are unknown.
SELECT snowflakeToDateTime(1426860704886947840::Int64, 'UTC'); -- { serverError UNKNOWN_FUNCTION }
SELECT snowflakeToDateTime64(1426860704886947840::Int64, 'UTC'); -- { serverError UNKNOWN_FUNCTION }
SELECT dateTimeToSnowflake(toDateTime('2021-08-15 18:57:56', 'UTC')); -- { serverError UNKNOWN_FUNCTION }
SELECT dateTime64ToSnowflake(toDateTime64('2021-08-15 18:57:56.492', 3, 'UTC')); -- { serverError UNKNOWN_FUNCTION }

-- The replacement functions remain available.
SELECT snowflakeIDToDateTime(7204436857747984384::UInt64) FORMAT Null;
SELECT snowflakeIDToDateTime64(7204436857747984384::UInt64) FORMAT Null;
SELECT dateTimeToSnowflakeID(toDateTime('2021-08-15 18:57:56', 'UTC')) FORMAT Null;
SELECT dateTime64ToSnowflakeID(toDateTime64('2021-08-15 18:57:56.492', 3, 'UTC')) FORMAT Null;
