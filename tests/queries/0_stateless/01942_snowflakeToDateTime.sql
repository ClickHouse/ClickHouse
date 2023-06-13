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


DROP TABLE IF EXISTS tab;
CREATE TABLE tab(tz String, val Int64) engine=Log;
INSERT INTO tab VALUES ('Asia/Singapore', 42);

SELECT * FROM tab WHERE snowflakeToDateTime(42::Int64, tz) != now() SETTINGS allow_nonconst_timezone_arguments = 1;
SELECT * FROM tab WHERE snowflakeToDateTime64(42::Int64, tz) != now() SETTINGS allow_nonconst_timezone_arguments = 1;

DROP TABLE tab;
