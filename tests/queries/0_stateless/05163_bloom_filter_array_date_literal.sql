-- The `bloom_filter` skip index hashes the elements of a constant array argument of `hasAny`/`hasAll`
-- after converting them to the indexed type. A `Date` literal carries a unit, so converting it without
-- its own type re-based the day number as seconds and no granule matched; a `Date32` literal took the
-- throwing path of the same conversion instead. The index must answer as the function does.

-- A `Date32` literal and a `DateTime` column meet in a `DateTime64` supertype, and the literal is
-- converted with the session's time zone, so the comparison is only time-zone independent when the
-- session time zone is fixed.
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_bf_array_date;

CREATE TABLE t_bf_array_date (id UInt64, arr Array(DateTime('UTC')), INDEX bf arr TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_bf_array_date SELECT number, [toDateTime('2030-05-05 05:05:05', 'UTC') + number] FROM numbers(40) WHERE number != 21;
INSERT INTO t_bf_array_date VALUES (21, [toDateTime('2024-01-02 00:00:00', 'UTC')]);
OPTIMIZE TABLE t_bf_array_date FINAL;

SELECT 'Array(DateTime) with a Date literal';
SELECT count() FROM t_bf_array_date WHERE hasAny(arr, [toDate('2024-01-02')]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_array_date WHERE hasAny(arr, [toDate('2024-01-02')]);
SELECT count() FROM t_bf_array_date WHERE hasAll(arr, [toDate('2024-01-02')]);
SELECT count() FROM t_bf_array_date WHERE has([toDate('2024-01-02')], arr[1]);

SELECT 'Array(DateTime) with a Date32 literal';
SELECT count() FROM t_bf_array_date WHERE hasAny(arr, [toDate32('2024-01-02')]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_array_date WHERE hasAny(arr, [toDate32('2024-01-02')]);

SELECT 'a same-type literal still finds the row';
SELECT count() FROM t_bf_array_date WHERE hasAny(arr, [toDateTime('2024-01-02 00:00:00', 'UTC')]);
SELECT count() FROM t_bf_array_date WHERE hasAny(arr, [toDate('2024-01-03')]);

DROP TABLE t_bf_array_date;

SELECT 'Array(DateTime64) with a Date literal';

CREATE TABLE t_bf_array_dt64 (id UInt64, arr Array(DateTime64(3, 'UTC')), INDEX bf arr TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_bf_array_dt64 SELECT number, [toDateTime64('2030-05-05 05:05:05', 3, 'UTC') + number] FROM numbers(40) WHERE number != 21;
INSERT INTO t_bf_array_dt64 VALUES (21, [toDateTime64('2024-01-02 00:00:00', 3, 'UTC')]);
OPTIMIZE TABLE t_bf_array_dt64 FINAL;

SELECT count() FROM t_bf_array_dt64 WHERE hasAny(arr, [toDate('2024-01-02')]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_array_dt64 WHERE hasAny(arr, [toDate('2024-01-02')]);

DROP TABLE t_bf_array_dt64;
