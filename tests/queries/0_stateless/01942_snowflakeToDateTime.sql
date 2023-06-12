-- -- Error cases
SELECT snowflakeToDateTime();  -- {serverError 42}
SELECT snowflakeToDateTime64();  -- {serverError 42}

SELECT snowflakeToDateTime('abc');  -- {serverError 43}
SELECT snowflakeToDateTime64('abc');  -- {serverError 43}

SELECT snowflakeToDateTime('abc', 123);  -- {serverError 43}
SELECT snowflakeToDateTime64('abc', 123);  -- {serverError 43}

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

SELECT materialize('Asia/Singapore') a, snowflakeToDateTime(649::Int64, a) settings allow_nonconst_timezone_arguments = 1
