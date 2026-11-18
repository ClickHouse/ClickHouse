-- The conversion of `Date` (which is `UInt16`) to `Int16` wraps around for the dates after 2059-09-18,
-- therefore it is not monotonic on the whole range of `Date`, and the primary key analysis must not use it.
-- https://github.com/ClickHouse/ClickHouse/issues/72580

DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (c0 Date) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_date VALUES ('1970-01-01'), ('2024-10-04'), ('2057-08-12'), ('2079-07-08'), ('2134-04-11');

SELECT c0, toInt16(c0) FROM t_date ORDER BY c0;

SELECT toInt16(c0) AS x FROM t_date WHERE x > 30146 ORDER BY x;
SELECT count() FROM t_date WHERE toInt16(c0) > 30146;

DROP VIEW IF EXISTS v_date;
CREATE VIEW v_date AS SELECT toInt16(c0) AS c0 FROM t_date;
SELECT c0 FROM v_date WHERE c0 > 30146 ORDER BY c0 DESC;

-- The same for `DateTime` (which is `UInt32`) to `Int32`, wrapping around after 2038-01-19.

DROP TABLE IF EXISTS t_datetime;
CREATE TABLE t_datetime (c0 DateTime('UTC')) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_datetime VALUES ('1970-01-01 00:00:00'), ('2033-05-18 03:33:20'), ('2065-01-24 05:20:00'), ('2096-10-02 07:06:40');

SELECT toInt32(c0) AS x FROM t_datetime WHERE x > 1500000000 ORDER BY x;

-- When the data does not cross the wrap-around point, the conversion is monotonic
-- and the primary key is still used.

DROP TABLE IF EXISTS t_date_no_wrap;
CREATE TABLE t_date_no_wrap (c0 Date) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_date_no_wrap VALUES ('1970-01-01'), ('2024-10-04'), ('2057-08-12');

SELECT toInt16(c0) AS x FROM t_date_no_wrap WHERE x > 30146 ORDER BY x SETTINGS force_primary_key = 1;

DROP VIEW v_date;
DROP TABLE t_date;
DROP TABLE t_datetime;
DROP TABLE t_date_no_wrap;
