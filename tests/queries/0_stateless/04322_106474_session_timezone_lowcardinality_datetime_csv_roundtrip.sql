-- Tags: no-replicated-database, no-parallel
-- Tag no-replicated-database: user_files
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106474
-- `session_timezone` was ignored when serializing `LowCardinality(DateTime)`
-- to text formats. The dictionary serializer was looked up from the global
-- `SerializationObjectPool` using only the dictionary type name as the key,
-- so the first cached `SerializationLowCardinality(DateTime)` froze whichever
-- effective timezone happened to be in scope first; subsequent queries with
-- a different `session_timezone` reused that frozen serializer and emitted
-- the wrong wall-clock string. Plain `DateTime` did not hit this because its
-- pool key already includes the resolved timezone.

SET allow_suspicious_low_cardinality_types = 1;
SET engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_106474_lc;
DROP TABLE IF EXISTS t_106474_dt;

-- Create the table and insert under the server default timezone. The unix
-- timestamp stored in both tables is identical (`toDateTime('2026-06-01 10:00:00')`
-- under the server default).
CREATE TABLE t_106474_lc (c0 LowCardinality(DateTime)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t_106474_dt (c0 DateTime) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO t_106474_lc (c0) VALUES ('2026-06-01 10:00:00');
INSERT INTO t_106474_dt (c0) SELECT c0 FROM t_106474_lc;

-- Pre-warm the `SerializationLowCardinality(DateTime)` cache entry under the
-- server default timezone by formatting the column as text. Without the fix,
-- this freezes the cached nested `SerializationDateTime` to the server
-- timezone and every later query that writes `LowCardinality(DateTime)` ignores
-- `session_timezone`.
SELECT c0 FROM t_106474_lc FORMAT TSV;

-- Write both columns to CSV files under `session_timezone = 'Asia/Thimphu'`
-- (UTC+6). They came from the same unix timestamp, so the two CSV files must
-- contain the SAME wall-clock string.
SET session_timezone = 'Asia/Thimphu';
INSERT INTO TABLE FUNCTION file('04322_106474/t_lc.csv', 'CSV', 'c0 LowCardinality(DateTime)') SELECT c0 FROM t_106474_lc;
INSERT INTO TABLE FUNCTION file('04322_106474/t_dt.csv', 'CSV', 'c0 DateTime') SELECT c0 FROM t_106474_dt;

SELECT 'thimphu-bytes-equal' AS label,
       (SELECT line FROM file('04322_106474/t_lc.csv', 'LineAsString', 'line String'))
     = (SELECT line FROM file('04322_106474/t_dt.csv', 'LineAsString', 'line String')) AS equal;

-- Also exercise a different timezone in the same session. With the previous
-- pool key the cached `SerializationLowCardinality(DateTime)` still held
-- the first timezone, so this second write would emit the same (UTC) string.
-- With the fixed pool key (which includes the inner serializer's
-- timezone-aware hash), each effective timezone gets its own cache entry.
SET session_timezone = 'Asia/Tokyo';
INSERT INTO TABLE FUNCTION file('04322_106474/t_lc_tokyo.csv', 'CSV', 'c0 LowCardinality(DateTime)') SELECT c0 FROM t_106474_lc;
INSERT INTO TABLE FUNCTION file('04322_106474/t_dt_tokyo.csv', 'CSV', 'c0 DateTime') SELECT c0 FROM t_106474_dt;

SELECT 'tokyo-bytes-equal' AS label,
       (SELECT line FROM file('04322_106474/t_lc_tokyo.csv', 'LineAsString', 'line String'))
     = (SELECT line FROM file('04322_106474/t_dt_tokyo.csv', 'LineAsString', 'line String')) AS equal;

DROP TABLE t_106474_lc;
DROP TABLE t_106474_dt;
