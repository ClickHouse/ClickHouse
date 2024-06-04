-- https://github.com/ClickHouse/ClickHouse/issues/64543
DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS bar;
CREATE TABLE foo (id UInt64, seq UInt64) ENGINE = Memory;
CREATE TABLE bar (id UInt64, seq UInt64, name String) ENGINE = ReplacingMergeTree ORDER BY id;
INSERT INTO foo VALUES (1, 1);
INSERT INTO bar VALUES (1, 1, 'a') (2, 2, 'b');
INSERT INTO bar VALUES (1, 2, 'b') (2, 3, 'c');
SELECT * FROM bar INNER JOIN foo USING id WHERE bar.seq > foo.seq SETTINGS final = 1;

-- Same problem possible can happen with array join
DROP TABLE IF EXISTS t;
CREATE TABLE t (k1 UInt64, k2 UInt64, v UInt64) ENGINE = ReplacingMergeTree() ORDER BY (k1, k2);
SET optimize_on_insert = 0;
INSERT INTO t VALUES (1, 2, 3) (1, 2, 4) (2, 3, 4), (2, 3, 5);
-- { echo ON }
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 1 ORDER BY row SETTINGS enable_vertical_final = 0;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 1 ORDER BY row SETTINGS enable_vertical_final = 1;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 2 ORDER BY row SETTINGS enable_vertical_final = 0;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 2 ORDER BY row SETTINGS enable_vertical_final = 1;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 3 ORDER BY row SETTINGS enable_vertical_final = 0;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 3 ORDER BY row SETTINGS enable_vertical_final = 1;
