-- toDateTime64, toDateTime with a scale and toTime64 return NULL for a NULL row of a Nullable(String) argument.
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_nullable_string_to_dt64;
CREATE TABLE t_nullable_string_to_dt64 (id UInt8, s Nullable(String), fs Nullable(FixedString(23)), t Nullable(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nullable_string_to_dt64 VALUES (1, '2025-01-01 00:00:00.123', '2025-01-01 00:00:00.123', '12:34:56.789'), (2, NULL, NULL, NULL);

SELECT id, toDateTime64(s, 3), toDateTime64(s, 3, 'Asia/Tokyo'), toDateTime(s, 3), toDateTime64(fs, 3), toDateTime64(toLowCardinality(s), 3), toTime64(t, 3)
FROM t_nullable_string_to_dt64 ORDER BY id SETTINGS cast_string_to_date_time_mode = 'best_effort';
SELECT id, toDateTime64(s, 3), toDateTime64(s, 3, 'Asia/Tokyo'), toDateTime(s, 3), toDateTime64(fs, 3), toDateTime64(toLowCardinality(s), 3), toTime64(t, 3)
FROM t_nullable_string_to_dt64 ORDER BY id SETTINGS cast_string_to_date_time_mode = 'best_effort_us';
SELECT id, toDateTime64(s, 3), toDateTime64(s, 3, 'Asia/Tokyo'), toDateTime(s, 3), toDateTime64(fs, 3), toDateTime64(toLowCardinality(s), 3), toTime64(t, 3)
FROM t_nullable_string_to_dt64 ORDER BY id SETTINGS cast_string_to_date_time_mode = 'basic';

-- Every row is NULL.
SELECT id, toDateTime64(s, 3), toTime64(t, 3) FROM t_nullable_string_to_dt64 WHERE id = 2 SETTINGS cast_string_to_date_time_mode = 'best_effort';
SELECT toDateTime64(NULL::Nullable(String), 3), toTime64(NULL::Nullable(String), 3);

-- NULL rows interleaved with values over many blocks: each value stays in its row.
SELECT countIf(isNull(x)), sum(toUnixTimestamp64Milli(x)) = sumIf(number * 1000 + 123, number % 3 != 0)
FROM (SELECT number, toDateTime64(if(number % 3 = 0, NULL, concat(toString(toDateTime(number)), '.123')), 3) AS x FROM numbers(100000))
SETTINGS cast_string_to_date_time_mode = 'best_effort', max_block_size = 1000;

-- A value that is not NULL and cannot be parsed still throws.
SELECT toDateTime64(materialize(CAST('' AS Nullable(String))), 3) SETTINGS cast_string_to_date_time_mode = 'best_effort'; -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime64(materialize(CAST('garbage' AS Nullable(String))), 3); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toTime64(materialize(CAST('' AS Nullable(String))), 3); -- { serverError CANNOT_PARSE_DATETIME }

DROP TABLE t_nullable_string_to_dt64;
