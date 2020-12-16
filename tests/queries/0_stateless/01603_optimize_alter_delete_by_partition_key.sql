SET optimize_alter_delete_by_partition_key = 'true';
CREATE TABLE t1 (`date` Date, `x` UInt64, `s` String) ENGINE = MergeTree PARTITION BY date ORDER BY (date, x);
INSERT INTO t1 SELECT toDate('2020-12-01'), number, cast(number as String) from system.numbers limit 10000;
INSERT INTO t1 SELECT toDate('2020-12-02'), number, cast(number as String) from system.numbers limit 20000;
SELECT date, COUNT(1) FROM t1 GROUP BY date ORDER BY date;
ALTER TABLE t1 DELETE WHERE date = '2020-12-01';
SELECT date, COUNT(1) FROM t1 GROUP BY date ORDER BY date;
DROP TABLE t1;

CREATE TABLE t2 (`date` Date, `x` UInt64, `s` String) ENGINE = MergeTree PARTITION BY toYYYYMMDD(date) ORDER BY (date, x);
INSERT INTO t2 SELECT toDate('2020-12-01'), number, cast(number as String) from system.numbers limit 10000;
INSERT INTO t2 SELECT toDate('2020-12-02'), number, cast(number as String) from system.numbers limit 20000;
SELECT date, COUNT(1) FROM t2 GROUP BY date ORDER BY date;
ALTER TABLE t2 DELETE WHERE date = '2020-12-01';
SELECT date, COUNT(1) FROM t2 GROUP BY date ORDER BY date;
DROP TABLE t2;