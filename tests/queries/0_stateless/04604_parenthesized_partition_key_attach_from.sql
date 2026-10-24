-- Tables whose keys were written with redundant parentheses (`PARTITION BY (a)`) must be
-- interchangeable with tables whose keys were written without them (`PARTITION BY a`):
-- the keys are the same, so `ATTACH PARTITION FROM` must not fail with
-- "Tables have different partition key" or "Tables have different ordering".
-- https://github.com/ClickHouse/ClickHouse/pull/92340 broke this by preserving the parentheses.

DROP TABLE IF EXISTS t_src_04604;
DROP TABLE IF EXISTS t_dst_04604;

CREATE TABLE t_src_04604 (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY (a) PRIMARY KEY (a) ORDER BY (a, b) SAMPLE BY (a);
CREATE TABLE t_dst_04604 (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY a PRIMARY KEY a ORDER BY (a, b) SAMPLE BY a;

INSERT INTO t_src_04604 VALUES (1, 1), (1, 2), (2, 1);

ALTER TABLE t_dst_04604 ATTACH PARTITION 1 FROM t_src_04604;
SELECT * FROM t_dst_04604 ORDER BY a, b;

DROP TABLE t_src_04604;
DROP TABLE t_dst_04604;
