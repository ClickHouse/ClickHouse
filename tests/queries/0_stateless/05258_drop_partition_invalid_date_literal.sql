-- A partition literal that is not a valid, in-range value of a `Date`/`Date32`/`DateTime`/`DateTime64` partition key
-- must be rejected instead of being parsed leniently into another, existing partition.
-- https://github.com/ClickHouse/ClickHouse/issues/122288

DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (d Date, x UInt8) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_date VALUES ('1970-01-01', 1), ('2024-02-29', 2), ('2024-03-01', 3), ('2024-12-01', 4), ('2149-06-06', 5);

-- A day that does not exist used to roll over into the next month and drop `2024-03-01`.
ALTER TABLE t_date DROP PARTITION '2024-02-30'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_date DROP PARTITION '2024-04-31'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_date DETACH PARTITION '2024-02-30'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_date DELETE IN PARTITION '2024-02-30' WHERE 1; -- { serverError INVALID_PARTITION_VALUE }
-- A month that does not exist, or the zero date, used to drop `1970-01-01`.
ALTER TABLE t_date DROP PARTITION '2024-13-01'; -- { serverError CANNOT_PARSE_DATE }
ALTER TABLE t_date DROP PARTITION '9999-99-99'; -- { serverError CANNOT_PARSE_DATE }
ALTER TABLE t_date DROP PARTITION '0000-00-00'; -- { serverError CANNOT_PARSE_DATE }
-- A date outside the range of `Date` used to be clamped to its boundary.
ALTER TABLE t_date DROP PARTITION '1900-01-01'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
ALTER TABLE t_date DROP PARTITION '9999-01-01'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- The setting does not matter.
ALTER TABLE t_date DROP PARTITION '2024-02-30' SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_date DROP PARTITION '9999-01-01' SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT 'date, nothing dropped', groupArray(x) FROM (SELECT x FROM t_date ORDER BY x);

-- Valid literals keep working: a valid date that names no partition is a no-op, and the other accepted forms still drop.
ALTER TABLE t_date DROP PARTITION '1999-12-31';
ALTER TABLE t_date DROP PARTITION '2024-02-29';
ALTER TABLE t_date DROP PARTITION '2024-3-1';
ALTER TABLE t_date DROP PARTITION '20241201';
ALTER TABLE t_date DROP PARTITION '2149-06-06';
SELECT 'date, valid literals', groupArray(x) FROM (SELECT x FROM t_date ORDER BY x);
DROP TABLE t_date;

DROP TABLE IF EXISTS t_date32;
CREATE TABLE t_date32 (d Date32, x UInt8) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_date32 VALUES ('1900-01-01', 1), ('2024-03-01', 2), ('2299-12-31', 3);
ALTER TABLE t_date32 DROP PARTITION '2024-02-30'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_date32 DROP PARTITION '2024-13-01'; -- { serverError CANNOT_PARSE_DATE }
SELECT 'date32, nothing dropped', groupArray(x) FROM (SELECT x FROM t_date32 ORDER BY x);
ALTER TABLE t_date32 DROP PARTITION '1900-01-01';
ALTER TABLE t_date32 DROP PARTITION '2024/03/01';
SELECT 'date32, valid literals', groupArray(x) FROM (SELECT x FROM t_date32 ORDER BY x);
DROP TABLE t_date32;

DROP TABLE IF EXISTS t_datetime;
CREATE TABLE t_datetime (d DateTime('UTC'), x UInt8) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_datetime VALUES ('1970-01-01 00:00:00', 1), ('2024-03-01 00:00:00', 2), ('2024-03-01 01:00:00', 3), ('2024-03-01 12:34:56', 4), ('2106-02-07 06:28:15', 5), ('2024-12-01 00:00:00', 6);
ALTER TABLE t_datetime DROP PARTITION '2024-02-30 00:00:00'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_datetime DROP PARTITION '2024-02-30'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_datetime DROP PARTITION '2024-02-29 25:00:00'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_datetime DROP PARTITION '2024-02-29 23:60:00'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_datetime DROP PARTITION '2024-13-01 00:00:00'; -- { serverError CANNOT_PARSE_DATETIME }
ALTER TABLE t_datetime DROP PARTITION '0000-00-00 00:00:00'; -- { serverError CANNOT_PARSE_DATETIME }
ALTER TABLE t_datetime DROP PARTITION '2200-01-01 00:00:00'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
ALTER TABLE t_datetime DROP PARTITION '1969-12-31 23:59:59'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
ALTER TABLE t_datetime DROP PARTITION '4294967296'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT 'datetime, nothing dropped', groupArray(x) FROM (SELECT x FROM t_datetime ORDER BY x);
ALTER TABLE t_datetime DROP PARTITION '2024-02-29 23:59:59';
ALTER TABLE t_datetime DROP PARTITION '2024-03-01';
ALTER TABLE t_datetime DROP PARTITION '1709254800';
ALTER TABLE t_datetime DROP PARTITION '2024-03-01T12:34:56';
ALTER TABLE t_datetime DROP PARTITION '2106-02-07 06:28:15';
SELECT 'datetime, valid literals', groupArray(x) FROM (SELECT x FROM t_datetime ORDER BY x);
DROP TABLE t_datetime;

DROP TABLE IF EXISTS t_datetime64;
CREATE TABLE t_datetime64 (d DateTime64(3, 'UTC'), x UInt8) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_datetime64 VALUES ('1900-01-01 00:00:00', 1), ('1970-01-01 00:00:00', 2), ('2024-03-01 00:00:00', 3), ('2024-03-01 01:00:00', 4), ('2024-03-01 12:34:56.789', 5);
ALTER TABLE t_datetime64 DROP PARTITION '2024-02-30 00:00:00'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_datetime64 DROP PARTITION '2024-02-29 25:00:00'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_datetime64 DROP PARTITION '2024-13-01 00:00:00'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_datetime64 DROP PARTITION '0000-00-00 00:00:00'; -- { serverError INVALID_PARTITION_VALUE }
SELECT 'datetime64, nothing dropped', groupArray(x) FROM (SELECT x FROM t_datetime64 ORDER BY x);
ALTER TABLE t_datetime64 DROP PARTITION '2024-03-01 00:00:00';
ALTER TABLE t_datetime64 DROP PARTITION '2024-03-01 12:34:56.789';
ALTER TABLE t_datetime64 DROP PARTITION '1970-01-01';
SELECT 'datetime64, valid literals', groupArray(x) FROM (SELECT x FROM t_datetime64 ORDER BY x);
DROP TABLE t_datetime64;

-- Each element of a tuple partition key is checked.
DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (d Date, k UInt8, x UInt8) ENGINE = MergeTree PARTITION BY (d, k) ORDER BY x;
INSERT INTO t_tuple VALUES ('2024-03-01', 1, 1), ('2024-02-29', 1, 2);
ALTER TABLE t_tuple DROP PARTITION ('2024-02-30', 1); -- { serverError INVALID_PARTITION_VALUE }
SELECT 'tuple, nothing dropped', groupArray(x) FROM (SELECT x FROM t_tuple ORDER BY x);
ALTER TABLE t_tuple DROP PARTITION ('2024-02-29', 1);
SELECT 'tuple, valid literal', groupArray(x) FROM (SELECT x FROM t_tuple ORDER BY x);
DROP TABLE t_tuple;
