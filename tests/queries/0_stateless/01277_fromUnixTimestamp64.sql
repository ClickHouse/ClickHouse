-- -- Error cases
SELECT fromUnixTimestamp64Milli();  -- {serverError 42}
SELECT fromUnixTimestamp64Micro();  -- {serverError 42}
SELECT fromUnixTimestamp64Nano();  -- {serverError 42}

SELECT fromUnixTimestamp64Milli('abc');  -- {serverError 43}
SELECT fromUnixTimestamp64Micro('abc');  -- {serverError 43}
SELECT fromUnixTimestamp64Nano('abc');  -- {serverError 43}

SELECT fromUnixTimestamp64Milli('abc', 123);  -- {serverError 43}
SELECT fromUnixTimestamp64Micro('abc', 123);  -- {serverError 43}
SELECT fromUnixTimestamp64Nano('abc', 123);  -- {serverError 43}

SELECT 'const column';
WITH
	CAST(1234567891011 AS Int64) AS i64,
	'UTC' AS tz
SELECT
	tz,
	i64,
	fromUnixTimestamp64Milli(i64, tz),
	fromUnixTimestamp64Micro(i64, tz),
	fromUnixTimestamp64Nano(i64, tz) as dt64,
	toTypeName(dt64);

WITH
	CAST(1234567891011 AS Int64) AS i64,
	'Asia/Makassar' AS tz
SELECT
	tz,
	i64,
	fromUnixTimestamp64Milli(i64, tz),
	fromUnixTimestamp64Micro(i64, tz),
	fromUnixTimestamp64Nano(i64, tz) as dt64,
	toTypeName(dt64);

SELECT 'non-const column';
WITH
	CAST(1234567891011 AS Int64) AS i64,
	'UTC' AS tz
SELECT
	i64,
	fromUnixTimestamp64Milli(i64, tz),
	fromUnixTimestamp64Micro(i64, tz),
	fromUnixTimestamp64Nano(i64, tz) as dt64;