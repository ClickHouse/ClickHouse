-- Test for https://github.com/ClickHouse/ClickHouse/issues/30383
-- A 5-character string after a date-only DateTime value caused wrong parsing
-- because the optimistic path in readDateTimeTextImpl could misinterpret
-- unrelated data beyond the value as part of the datetime.

DROP TABLE IF EXISTS t_datetime_short_string;
CREATE TABLE t_datetime_short_string (timestamp DateTime, name String, device_id UInt8) ENGINE = MergeTree ORDER BY timestamp;

-- The original failing case: 5-char string with space before it
INSERT INTO t_datetime_short_string VALUES ('2021-10-12', 'aaaaa', 4);

-- Variations with different string lengths (these always worked, included for regression coverage)
INSERT INTO t_datetime_short_string VALUES ('2021-10-12', 'a', 1);
INSERT INTO t_datetime_short_string VALUES ('2021-10-12', 'aa', 2);
INSERT INTO t_datetime_short_string VALUES ('2021-10-12', 'aaa', 3);
INSERT INTO t_datetime_short_string VALUES ('2021-10-12', 'aaaa', 5);
INSERT INTO t_datetime_short_string VALUES ('2021-10-12', 'aaaaaa', 6);

-- Without space before the string (this always worked)
INSERT INTO t_datetime_short_string VALUES ('2021-10-12','aaaaa', 7);

-- Full datetime format should still work
INSERT INTO t_datetime_short_string VALUES ('2021-10-12 01:02:03', 'aaaaa', 8);

SELECT timestamp, name, device_id FROM t_datetime_short_string ORDER BY device_id;

DROP TABLE t_datetime_short_string;

-- Also test with inline format parsing
SELECT * FROM format(CSV, 'timestamp DateTime, name String, device_id UInt8', '2021-10-12,aaaaa,4');
SELECT * FROM format(TSV, 'timestamp DateTime, name String, device_id UInt8', '2021-10-12\taaaaa\t4');
