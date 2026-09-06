-- The time zone argument can be a non-constant LowCardinality column. Its dictionary always
-- contains the default value (the empty string), which is not a valid time zone, so the function
-- must not be executed on the whole dictionary as is.

SELECT DISTINCT formatDateTime(toDateTime('2026-08-29 00:00:00', 'UTC'), '%F %T', toLowCardinality(if(number % 2, 'UTC', 'Europe/Amsterdam'))) AS x FROM numbers(4) ORDER BY x;
SELECT DISTINCT formatDateTimeInJodaSyntax(toDateTime('2026-08-29 00:00:00', 'UTC'), 'yyyy-MM-dd HH:mm:ss', toLowCardinality(if(number % 2, 'UTC', 'Europe/Amsterdam'))) AS x FROM numbers(4) ORDER BY x;
SELECT DISTINCT fromUnixTimestamp(toUInt32(1787961600), '%F %T', toLowCardinality(if(number % 2, 'UTC', 'Europe/Amsterdam'))) AS x FROM numbers(4) ORDER BY x;
SELECT DISTINCT fromUnixTimestampInJodaSyntax(toUInt32(1787961600), 'yyyy-MM-dd HH:mm:ss', toLowCardinality(if(number % 2, 'UTC', 'Europe/Amsterdam'))) AS x FROM numbers(4) ORDER BY x;

SELECT formatDateTime(toDateTime64('2026-08-29 00:00:00.123', 3, 'UTC'), '%F %T.%f', materialize(toLowCardinality('Asia/Istanbul')));
SELECT toTypeName(formatDateTime(toDateTime('2026-08-29 00:00:00', 'UTC'), '%F %T', materialize(toLowCardinality('UTC'))));

-- An empty time zone is still an error.
SELECT formatDateTime(toDateTime('2026-08-29 00:00:00', 'UTC'), '%F %T', materialize(toLowCardinality(''))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
