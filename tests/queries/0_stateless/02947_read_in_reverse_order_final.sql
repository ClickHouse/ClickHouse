SET final=1;
SET optimize_read_in_order=1;

DROP TABLE IF EXISTS t_02947_reverse_order_final;

-- ReplacingMergeTree()
CREATE TABLE t_02947_reverse_order_final (x int, y int, z int)
ENGINE = ReplacingMergeTree ORDER BY (x, y);

-- Level-0 part with duplicate rows
INSERT INTO t_02947_reverse_order_final SETTINGS optimize_on_insert = 0 VALUES (0, 0, 0), (0, 0, 1);

SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC, y DESC;

INSERT INTO t_02947_reverse_order_final SETTINGS optimize_on_insert = 0 VALUES (0, 0, 0), (0, 0, 1), (0, 0, 2);

SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC, y DESC;

TRUNCATE t_02947_reverse_order_final;

INSERT INTO t_02947_reverse_order_final SELECT number, number, number FROM numbers(10000);
INSERT INTO t_02947_reverse_order_final SELECT number, number * 2, number FROM numbers(10000);
INSERT INTO t_02947_reverse_order_final SELECT number, number * 2, number * 3 FROM numbers(10000);

SELECT '---';
SELECT if(explain like '%ReadType: InReverseOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC, y, z LIMIT 1
    SETTINGS max_threads = 1, max_final_threads = 1
) WHERE explain like '%ReadType%';

SELECT '---';
SELECT count() FROM t_02947_reverse_order_final FINAL;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x, y, z LIMIT 1;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC, y, z LIMIT 1;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x, y DESC, z LIMIT 1;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC, y DESC, z LIMIT 1;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC, y DESC, z DESC LIMIT 1;

DROP TABLE t_02947_reverse_order_final;

-- ReplacingMergeTree(version)
CREATE TABLE t_02947_reverse_order_final (x int, y int, ver int)
ENGINE = ReplacingMergeTree(ver) ORDER BY x;

-- Level-0 part with duplicate rows
INSERT INTO t_02947_reverse_order_final SETTINGS optimize_on_insert = 0 VALUES (0, 0, 0), (0, 1, 0), (1, 0, 0), (1, 1, 1);

SELECT '---';
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC;

DROP TABLE t_02947_reverse_order_final;

-- ReplacingMergeTree(version, is_deleted)
CREATE TABLE t_02947_reverse_order_final (x int, y int, ver int, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY x;

INSERT INTO t_02947_reverse_order_final SETTINGS optimize_on_insert = 0 VALUES (0, 0, 0, 0), (0, 1, 0, 0), (1, 2, 0, 0), (1, 3, 0, 0);
OPTIMIZE TABLE t_02947_reverse_order_final FINAL SETTINGS mutations_sync = 2;
ALTER TABLE t_02947_reverse_order_final UPDATE is_deleted = 1 WHERE x = 1 SETTINGS mutations_sync = 2;

SELECT '---';
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC;

DROP TABLE t_02947_reverse_order_final;

CREATE TABLE t_02947_reverse_order_final (x int, y int, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY x;

INSERT INTO t_02947_reverse_order_final SELECT number, number, 1 FROM numbers(10000);
INSERT INTO t_02947_reverse_order_final SELECT number * 2, 0, -1 FROM numbers(5000);

SELECT '---';
SELECT count() FROM t_02947_reverse_order_final FINAL;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x ASC LIMIT 2;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC LIMIT 2;

DROP TABLE t_02947_reverse_order_final;

CREATE TABLE t_02947_reverse_order_final (x int, y int, sign Int8, version UInt32)
ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY x;

INSERT INTO t_02947_reverse_order_final SELECT number, number, 1, number FROM numbers(10000);
INSERT INTO t_02947_reverse_order_final VALUES (0, 0, -1, 0), (9998, 0, 1, 2), (9999, 0, -1, 9999);

SELECT '---';
SELECT count() FROM t_02947_reverse_order_final FINAL;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x ASC LIMIT 2;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC, version ASC LIMIT 2;

DROP TABLE t_02947_reverse_order_final;

CREATE TABLE t_02947_reverse_order_final (x int, y int)
ENGINE = SummingMergeTree() ORDER BY x;

INSERT INTO t_02947_reverse_order_final SELECT number, number FROM numbers(10000);
INSERT INTO t_02947_reverse_order_final SELECT number, number * 2 FROM numbers(10000);

SELECT '---';
SELECT count() FROM t_02947_reverse_order_final FINAL;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x ASC LIMIT 2;
SELECT * FROM t_02947_reverse_order_final FINAL ORDER BY x DESC LIMIT 2;

DROP TABLE t_02947_reverse_order_final;

CREATE TABLE t_02947_reverse_order_final (x int, y AggregateFunction(max, Int64))
ENGINE = AggregatingMergeTree() ORDER BY x;

INSERT INTO t_02947_reverse_order_final SELECT number, arrayReduce('maxState', [number::Int64]) FROM numbers(10000);
INSERT INTO t_02947_reverse_order_final SELECT number, arrayReduce('maxState', [number::Int64 * 2]) FROM numbers(5000);
INSERT INTO t_02947_reverse_order_final SELECT number * 2, arrayReduce('maxState', [number::Int64 * 3]) FROM numbers(5000);

SELECT '---';
SELECT count() FROM t_02947_reverse_order_final FINAL;
SELECT x, maxMerge(y) FROM t_02947_reverse_order_final FINAL GROUP BY x ORDER BY x ASC LIMIT 1 OFFSET 1;
SELECT x, maxMerge(y) FROM t_02947_reverse_order_final FINAL GROUP BY x ORDER BY x DESC LIMIT 2;