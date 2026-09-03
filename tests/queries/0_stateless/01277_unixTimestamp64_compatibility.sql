WITH
	toDateTime64('2019-09-16 19:20:12.345678910', 3) AS dt64
SELECT
	dt64,
	fromUnixTimestamp64Milli(toUnixTimestamp64Milli(dt64)),
	fromUnixTimestamp64Micro(toUnixTimestamp64Micro(dt64)),
	fromUnixTimestamp64Nano(toUnixTimestamp64Nano(dt64));

WITH
	toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT
	dt64,
	fromUnixTimestamp64Milli(toUnixTimestamp64Milli(dt64)),
	fromUnixTimestamp64Micro(toUnixTimestamp64Micro(dt64)),
	fromUnixTimestamp64Nano(toUnixTimestamp64Nano(dt64));

WITH
	toDateTime64('2019-09-16 19:20:12.345678910', 9) AS dt64
SELECT
	dt64,
	fromUnixTimestamp64Milli(toUnixTimestamp64Milli(dt64)),
	fromUnixTimestamp64Micro(toUnixTimestamp64Micro(dt64)),
	fromUnixTimestamp64Nano(toUnixTimestamp64Nano(dt64));

SELECT 'with explicit timezone';
WITH
	'UTC' as timezone,
	toDateTime64('2019-09-16 19:20:12.345678910', 3, timezone) AS dt64
SELECT
	dt64,
	fromUnixTimestamp64Milli(toUnixTimestamp64Milli(dt64), timezone),
	fromUnixTimestamp64Micro(toUnixTimestamp64Micro(dt64), timezone),
	fromUnixTimestamp64Nano(toUnixTimestamp64Nano(dt64), timezone) AS v,
	toTypeName(v);

WITH
	'Asia/Makassar' as timezone,
	toDateTime64('2019-09-16 19:20:12.345678910', 3, timezone) AS dt64
SELECT
	dt64,
	fromUnixTimestamp64Milli(toUnixTimestamp64Milli(dt64), timezone),
	fromUnixTimestamp64Micro(toUnixTimestamp64Micro(dt64), timezone),
	fromUnixTimestamp64Nano(toUnixTimestamp64Nano(dt64), timezone) AS v,
	toTypeName(v);


WITH
	CAST(1234567891011 AS Int64) AS val
SELECT
	val,
	toUnixTimestamp64Milli(fromUnixTimestamp64Milli(val)),
	toUnixTimestamp64Micro(fromUnixTimestamp64Micro(val)),
	toUnixTimestamp64Nano(fromUnixTimestamp64Nano(val));

SELECT 'with explicit timezone';
WITH
	'UTC' as timezone,
	CAST(1234567891011 AS Int64) AS val
SELECT
	val,
	toUnixTimestamp64Milli(fromUnixTimestamp64Milli(val, timezone)),
	toUnixTimestamp64Micro(fromUnixTimestamp64Micro(val, timezone)),
	toUnixTimestamp64Nano(fromUnixTimestamp64Nano(val, timezone)) AS v,
	toTypeName(v);