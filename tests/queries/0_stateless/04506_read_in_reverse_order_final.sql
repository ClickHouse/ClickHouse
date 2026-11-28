-- Reading in reverse order of the sorting key with FINAL for ReplacingMergeTree.
SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1;
SET optimize_read_in_reverse_order_final = 1;

DROP TABLE IF EXISTS t_reverse_final;

-- ReplacingMergeTree without a version column: the last inserted row wins.
CREATE TABLE t_reverse_final (x Int32, y Int32, z Int32)
ENGINE = ReplacingMergeTree ORDER BY (x, y);

-- Keep the parts unmerged so that the tests below really exercise level-0 parts and merging across parts.
SYSTEM STOP MERGES t_reverse_final;

-- Duplicate keys inside a single level-0 (unmerged) part.
INSERT INTO t_reverse_final SETTINGS optimize_on_insert = 0 VALUES (0, 0, 0), (0, 0, 1);

SELECT 'level-0 duplicates';
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC;

-- Another level-0 part with the same key: the row from the newest part wins.
INSERT INTO t_reverse_final SETTINGS optimize_on_insert = 0 VALUES (0, 0, 2), (0, 0, 3);

SELECT 'duplicates across parts';
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC;

-- Each row inserted separately: several distinct parts with the same key. The row from the newest part wins.
TRUNCATE TABLE t_reverse_final;
INSERT INTO t_reverse_final VALUES (0, 0, 10);
INSERT INTO t_reverse_final VALUES (0, 0, 11);
INSERT INTO t_reverse_final VALUES (0, 0, 12);
INSERT INTO t_reverse_final VALUES (0, 0, 13);

SELECT 'one part per row';
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC;

SYSTEM START MERGES t_reverse_final;
TRUNCATE TABLE t_reverse_final;

INSERT INTO t_reverse_final SELECT number, number, number FROM numbers(10000);
INSERT INTO t_reverse_final SELECT number, number * 2, number FROM numbers(10000);
INSERT INTO t_reverse_final SELECT number, number * 2, number * 3 FROM numbers(10000);

SELECT 'plan';
SELECT if(explain like '%ReadType: InReverseOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC LIMIT 1
) WHERE explain like '%ReadType%';

SELECT 'ASC and DESC results';
SELECT count() FROM t_reverse_final FINAL;
SELECT * FROM t_reverse_final FINAL ORDER BY x, y, z LIMIT 1;
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y, z LIMIT 1;
SELECT * FROM t_reverse_final FINAL ORDER BY x, y DESC, z LIMIT 1;
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC, z LIMIT 1;
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC, z DESC LIMIT 1;

-- The whole result (including its order) is the same with and without the optimization,
-- both in a single stream and with parallel FINAL streams.
SELECT 'same result with and without the optimization';
SELECT (SELECT cityHash64(groupArray((x, y, z))) FROM (SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC, z DESC SETTINGS max_threads = 1, max_final_threads = 1))
     = (SELECT cityHash64(groupArray((x, y, z))) FROM (SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC, z DESC SETTINGS optimize_read_in_reverse_order_final = 0));
SELECT (SELECT cityHash64(groupArray((x, y, z))) FROM (SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC, z DESC SETTINGS max_threads = 4, max_final_threads = 4))
     = (SELECT cityHash64(groupArray((x, y, z))) FROM (SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC, z DESC SETTINGS optimize_read_in_reverse_order_final = 0));

-- Rows removed by a lightweight delete stay invisible when reading in reverse order.
SELECT 'lightweight delete';
DELETE FROM t_reverse_final WHERE x = 9999;
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC, y DESC LIMIT 1;

DROP TABLE t_reverse_final;

-- ReplacingMergeTree with a version column: the row with the max version wins,
-- and among rows with equal versions the last inserted one wins.
CREATE TABLE t_reverse_final (x Int32, y Int32, ver Int32)
ENGINE = ReplacingMergeTree(ver) ORDER BY x;

SYSTEM STOP MERGES t_reverse_final;

INSERT INTO t_reverse_final SETTINGS optimize_on_insert = 0 VALUES (0, 0, 0), (0, 1, 0), (1, 0, 9), (1, 1, 1), (2, 0, 3), (2, 1, 3);

SELECT 'version column';
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC;

-- Equal versions across parts: the row from the newest part wins.
INSERT INTO t_reverse_final SETTINGS optimize_on_insert = 0 VALUES (0, 2, 0), (1, 2, 9), (2, 2, 3);

SELECT 'version ties across parts';
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC;

DROP TABLE t_reverse_final;

-- A descending sorting key and an ascending query cancel the physical direction.
-- The parallel FINAL path must still use the reverse read and keep the newest version.
CREATE TABLE t_reverse_final (x Int32, y Int32, ver Int32)
ENGINE = ReplacingMergeTree(ver) ORDER BY (x DESC)
SETTINGS allow_experimental_reverse_key = 1;

INSERT INTO t_reverse_final SELECT number, 1, 1 FROM numbers(10000);
INSERT INTO t_reverse_final SELECT number, 2, 2 FROM numbers(10000);

SELECT 'descending sorting key';
SELECT if(explain like '%ReadType: InReverseOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_reverse_final FINAL ORDER BY x ASC LIMIT 1 SETTINGS max_threads = 4, max_final_threads = 4
) WHERE explain like '%ReadType%';
SELECT * FROM t_reverse_final FINAL ORDER BY x ASC LIMIT 2 SETTINGS max_threads = 4, max_final_threads = 4;
SELECT (SELECT cityHash64(groupArray((x, y, ver))) FROM (SELECT * FROM t_reverse_final FINAL ORDER BY x ASC SETTINGS max_threads = 4, max_final_threads = 4))
     = (SELECT cityHash64(groupArray((x, y, ver))) FROM (SELECT * FROM t_reverse_final FINAL ORDER BY x ASC SETTINGS max_threads = 4, max_final_threads = 4, optimize_read_in_reverse_order_final = 0));

DROP TABLE t_reverse_final;

-- ReplacingMergeTree with version and is_deleted columns.
CREATE TABLE t_reverse_final (x Int32, y Int32, ver Int32, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY x;

SYSTEM STOP MERGES t_reverse_final;

INSERT INTO t_reverse_final SETTINGS optimize_on_insert = 0 VALUES (0, 0, 0, 0), (1, 0, 0, 0), (2, 0, 0, 0);
INSERT INTO t_reverse_final SETTINGS optimize_on_insert = 0 VALUES (1, 1, 1, 1);

SELECT 'is_deleted';
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC;

DROP TABLE t_reverse_final;

-- A merged (level > 0) part and a newer level-0 part with non-overlapping key ranges:
-- exercises the whole-chunk pass-through optimization of ReplacingSortedAlgorithm in reverse order.
CREATE TABLE t_reverse_final (x Int32, y Int32)
ENGINE = ReplacingMergeTree ORDER BY x;

INSERT INTO t_reverse_final SELECT number, 1 FROM numbers(1000);
OPTIMIZE TABLE t_reverse_final FINAL;
SYSTEM STOP MERGES t_reverse_final;
INSERT INTO t_reverse_final SELECT number + 1000, 2 FROM numbers(1000);

SELECT 'non-overlapping parts';
SELECT count() FROM t_reverse_final FINAL;
SELECT * FROM t_reverse_final FINAL ORDER BY x DESC LIMIT 2;
SELECT (SELECT cityHash64(groupArray((x, y))) FROM (SELECT * FROM t_reverse_final FINAL ORDER BY x DESC))
     = (SELECT cityHash64(groupArray((x, y))) FROM (SELECT * FROM t_reverse_final FINAL ORDER BY x DESC SETTINGS optimize_read_in_reverse_order_final = 0));

DROP TABLE t_reverse_final;

-- The reverse order optimization must not apply to the other engines.
CREATE TABLE t_reverse_final (x Int32, y Int32, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY x;

INSERT INTO t_reverse_final VALUES (0, 0, 1), (1, 0, 1);
INSERT INTO t_reverse_final VALUES (1, 0, -1);

SELECT 'collapsing not in reverse';
SELECT if(explain like '%ReadType: InReverseOrder%', 'Error: ' || explain, 'Ok') FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_reverse_final FINAL ORDER BY x DESC LIMIT 1
) WHERE explain like '%ReadType%';
SELECT x, y FROM t_reverse_final FINAL ORDER BY x DESC;

DROP TABLE t_reverse_final;

-- The `Merge` engine does not pass a reverse order request with `FINAL` to its child tables
-- (see `ReadFromMerge::requestReadingInOrder`), so the optimization does not apply there,
-- but the result must still be correct.
CREATE TABLE t_reverse_final (x Int32, y Int32) ENGINE = ReplacingMergeTree ORDER BY x;
CREATE TABLE t_reverse_final_merge (x Int32, y Int32) ENGINE = Merge(currentDatabase(), '^t_reverse_final$');

SYSTEM STOP MERGES t_reverse_final;

INSERT INTO t_reverse_final SETTINGS optimize_on_insert = 0 VALUES (0, 0), (1, 0), (2, 0);
INSERT INTO t_reverse_final SETTINGS optimize_on_insert = 0 VALUES (1, 1);

SELECT 'merge engine';
SELECT * FROM t_reverse_final_merge FINAL ORDER BY x DESC;

DROP TABLE t_reverse_final_merge;
DROP TABLE t_reverse_final;
