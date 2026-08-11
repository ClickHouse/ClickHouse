-- Tags: no-parallel-replicas
-- ^ parallel replicas change the plan asserted in this test

-- Monotonicity of `toString` for date and time types, see `ToStringMonotonicity`.

DROP TABLE IF EXISTS t_time;
CREATE TABLE t_time (x Time) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_time VALUES ('-99:00:00'), ('-83:20:00'), ('-27:46:40'), ('-01:00:00');

-- `'-01:00:00'` is lexicographically less than `'-99:00:00'`, so the primary key must not exclude the part.
SELECT count() FROM t_time WHERE toString(x) >= '-99:00:00';
SELECT toString(x) FROM t_time ORDER BY toString(x) SETTINGS optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_dst;
CREATE TABLE t_dst (x DateTime('America/New_York')) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_dst SELECT toDateTime(1636264795 + number, 'America/New_York') FROM numbers(10);

-- Local time moves back from `01:59:59` to `01:00:00` in the middle of the part.
SELECT toString(toDateTime(1636264799, 'America/New_York')), toString(toDateTime(1636264800, 'America/New_York'));
SELECT count() FROM t_dst WHERE toString(x) >= '2021-11-07 01:59:00';
SELECT toString(x) FROM t_dst ORDER BY toString(x) SETTINGS optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (d Date, y UInt32) ENGINE = MergeTree ORDER BY (d, y);
INSERT INTO t_date SELECT toDate('2021-01-01') + intDiv(number, 4), number % 4 FROM numbers(20);

SELECT trimLeft(explain) FROM (
    EXPLAIN PLAN SELECT * FROM t_date ORDER BY toString(d), y SETTINGS optimize_read_in_order = 1
) WHERE explain LIKE '%sort description%';

DROP TABLE t_time;
DROP TABLE t_dst;
DROP TABLE t_utc;
DROP TABLE t_date;
