-- `toString(dt, tz)` formats in `tz`, so it is monotonic only if `tz` has a fixed offset, whatever the time zone of `dt`.
-- 2024-11-03 05:30 .. 06:50 UTC is 01:30 .. 01:50 EDT, then 01:00 .. 01:50 EST in America/New_York.

DROP TABLE IF EXISTS t_utc;
DROP TABLE IF EXISTS t_key;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_ny;

CREATE TABLE t_utc (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_utc SELECT toDateTime('2024-11-03 05:30:00', 'UTC') + number * 600 FROM numbers(9);

SELECT count() FROM t_utc WHERE toString(dt, 'America/New_York') = '2024-11-03 01:40:00';
SELECT count() FROM t_utc WHERE toString(dt, 'America/New_York') >= '2024-11-03 01:45:00';
SELECT toString(dt, 'America/New_York') AS s FROM t_utc ORDER BY s LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT toString(dt, 'America/New_York') AS s, count() FROM t_utc GROUP BY s ORDER BY s SETTINGS optimize_aggregation_in_order = 1;
SELECT count() FROM (SELECT DISTINCT toString(dt, 'America/New_York') FROM t_utc SETTINGS optimize_distinct_in_order = 1);
SELECT s FROM (SELECT toString(dt, 'America/New_York') AS s, dt FROM t_utc ORDER BY dt LIMIT 100) ORDER BY s LIMIT 3 SETTINGS optimize_sorting_by_input_stream_properties = 1;

-- A fixed-offset time zone argument keeps the index.
SELECT count() FROM t_utc WHERE toString(dt, 'UTC') = '2024-11-03 05:40:00' SETTINGS force_primary_key = 1, max_rows_to_read = 2;

CREATE TABLE t_key (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY toString(dt, 'America/New_York') SETTINGS index_granularity = 1;
INSERT INTO t_key SELECT toDateTime('2024-11-03 05:30:00', 'UTC') + number * 600 FROM numbers(9);
SELECT count() FROM t_key WHERE dt < toDateTime('2024-11-03 06:05:00', 'UTC');

CREATE TABLE t_part (dt DateTime('UTC')) ENGINE = MergeTree PARTITION BY toString(dt, 'America/New_York') ORDER BY tuple();
INSERT INTO t_part SELECT toDateTime('2024-11-03 05:30:00', 'UTC') + number * 600 FROM numbers(9);
SELECT count() FROM t_part WHERE dt < toDateTime('2024-11-03 06:05:00', 'UTC');

CREATE TABLE t_null (dt Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_null SELECT toDateTime('2024-11-03 05:30:00', 'UTC') + number * 600 FROM numbers(9);
INSERT INTO t_null VALUES (NULL);
SELECT count() FROM t_null WHERE toString(dt, 'America/New_York') = '2024-11-03 01:40:00';

-- The time zone argument, not the column's, decides: formatting a New York column in UTC is monotonic.
CREATE TABLE t_ny (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_ny SELECT toDateTime('2024-11-03 05:30:00', 'UTC') + number * 600 FROM numbers(9);
SELECT count() FROM t_ny WHERE toString(dt, 'UTC') = '2024-11-03 05:40:00' SETTINGS force_primary_key = 1, max_rows_to_read = 2;

DROP TABLE t_utc;
DROP TABLE t_key;
DROP TABLE t_part;
DROP TABLE t_null;
DROP TABLE t_ny;
