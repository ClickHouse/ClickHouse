-- Reading in reverse order of the sorting key with FINAL through a `Merge` table.
-- The `Merge` table passes the reverse order request to its children (see `ReadFromMerge::requestReadingInOrder`),
-- and the request is accepted only when every child can read in reverse order with FINAL.
SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1;
SET optimize_read_in_reverse_order_final = 1;

DROP TABLE IF EXISTS t_rf_a;
DROP TABLE IF EXISTS t_rf_b;
DROP TABLE IF EXISTS t_rf_agg;
DROP TABLE IF EXISTS t_rf_desc;
DROP TABLE IF EXISTS t_rf_merge;
DROP TABLE IF EXISTS t_rf_merge_mixed;
DROP TABLE IF EXISTS t_rf_merge_desc;

CREATE TABLE t_rf_a (x Int32, y Int32) ENGINE = ReplacingMergeTree ORDER BY x;
CREATE TABLE t_rf_b (x Int32, y Int32) ENGINE = ReplacingMergeTree ORDER BY x;
CREATE TABLE t_rf_merge (x Int32, y Int32) ENGINE = Merge(currentDatabase(), '^t_rf_[ab]$');

-- Keep the parts unmerged, so that FINAL really merges level-0 parts with duplicate keys.
SYSTEM STOP MERGES t_rf_a;
SYSTEM STOP MERGES t_rf_b;

-- Duplicate keys inside a level-0 part and across parts in both children. FINAL is applied per child,
-- so the key ranges of the children may overlap and both rows with the same `x` are returned.
INSERT INTO t_rf_a SETTINGS optimize_on_insert = 0 VALUES (0, 0), (1, 0), (1, 1), (2, 0);
INSERT INTO t_rf_a SETTINGS optimize_on_insert = 0 VALUES (2, 2), (3, 0);
INSERT INTO t_rf_b SETTINGS optimize_on_insert = 0 VALUES (1, 10), (3, 10), (3, 11), (4, 10);
INSERT INTO t_rf_b SETTINGS optimize_on_insert = 0 VALUES (4, 12);

SELECT 'plan: both children read in reverse order';
SELECT if(explain LIKE '%ReadType: InReverseOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_rf_merge FINAL ORDER BY x DESC LIMIT 3
) WHERE explain LIKE '%ReadType%';

SELECT 'reverse order';
SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC;
SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC LIMIT 3;

SELECT 'same result without the optimization';
SELECT (SELECT groupArray((x, y)) FROM (SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC))
     = (SELECT groupArray((x, y)) FROM (SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC SETTINGS optimize_read_in_order = 0));

SELECT 'direct order';
SELECT if(explain LIKE '%ReadType: InOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_rf_merge FINAL ORDER BY x ASC LIMIT 3
) WHERE explain LIKE '%ReadType%';
SELECT * FROM t_rf_merge FINAL ORDER BY x ASC, y ASC LIMIT 3;

SELECT 'setting disabled';
SELECT if(explain LIKE '%ReadType: InReverseOrder%', 'Error: ' || explain, 'Ok') FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_rf_merge FINAL ORDER BY x DESC LIMIT 3 SETTINGS optimize_read_in_reverse_order_final = 0
) WHERE explain LIKE '%ReadType%';
SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC LIMIT 3 SETTINGS optimize_read_in_reverse_order_final = 0;

-- A child whose engine cannot read in reverse order with FINAL disables the optimization for the whole `Merge` table.
CREATE TABLE t_rf_agg (x Int32, y Int32) ENGINE = AggregatingMergeTree ORDER BY x;
INSERT INTO t_rf_agg VALUES (5, 50), (6, 60);
CREATE TABLE t_rf_merge_mixed (x Int32, y Int32) ENGINE = Merge(currentDatabase(), '^t_rf_(a|agg)$');

SELECT 'mixed engines: not in reverse order';
SELECT if(explain LIKE '%ReadType: InReverseOrder%', 'Error: ' || explain, 'Ok') FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_rf_merge_mixed FINAL ORDER BY x DESC LIMIT 3
) WHERE explain LIKE '%ReadType%';
SELECT * FROM t_rf_merge_mixed FINAL ORDER BY x DESC LIMIT 3;

-- A descending sorting key: the reading direction is the direction of the sort description flipped by the key.
CREATE TABLE t_rf_desc (x Int32, y Int32) ENGINE = ReplacingMergeTree ORDER BY x DESC;
SYSTEM STOP MERGES t_rf_desc;
INSERT INTO t_rf_desc SETTINGS optimize_on_insert = 0 VALUES (0, 0), (1, 0), (1, 1), (2, 0);
INSERT INTO t_rf_desc SETTINGS optimize_on_insert = 0 VALUES (2, 2), (3, 0);
CREATE TABLE t_rf_merge_desc (x Int32, y Int32) ENGINE = Merge(currentDatabase(), '^t_rf_desc$');

SELECT 'descending sorting key: ORDER BY x ASC reads in reverse order';
SELECT if(explain LIKE '%ReadType: InReverseOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_rf_merge_desc FINAL ORDER BY x ASC LIMIT 2
) WHERE explain LIKE '%ReadType%';
SELECT * FROM t_rf_merge_desc FINAL ORDER BY x ASC LIMIT 2;
SELECT 'descending sorting key: ORDER BY x DESC reads in order';
SELECT if(explain LIKE '%ReadType: InOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_rf_merge_desc FINAL ORDER BY x DESC LIMIT 2
) WHERE explain LIKE '%ReadType%';
SELECT * FROM t_rf_merge_desc FINAL ORDER BY x DESC LIMIT 2;

DROP TABLE t_rf_merge_desc;
DROP TABLE t_rf_merge_mixed;
DROP TABLE t_rf_merge;
DROP TABLE t_rf_desc;
DROP TABLE t_rf_agg;
DROP TABLE t_rf_b;
DROP TABLE t_rf_a;

-- Larger children with several parts and granules each, so that the reverse reading spans several mark ranges
-- and several parts per child, and the reverse-sorted streams of the children are merged.
CREATE TABLE t_rf_a (x UInt32, y UInt32) ENGINE = ReplacingMergeTree ORDER BY x SETTINGS index_granularity = 128;
CREATE TABLE t_rf_b (x UInt32, y UInt32) ENGINE = ReplacingMergeTree ORDER BY x SETTINGS index_granularity = 128;
CREATE TABLE t_rf_merge (x UInt32, y UInt32) ENGINE = Merge(currentDatabase(), '^t_rf_[ab]$');

SYSTEM STOP MERGES t_rf_a;
SYSTEM STOP MERGES t_rf_b;

INSERT INTO t_rf_a SELECT number, 1 FROM numbers(10000);
INSERT INTO t_rf_a SELECT number * 2, 2 FROM numbers(5000);
INSERT INTO t_rf_b SELECT number + 5000, 3 FROM numbers(10000);
INSERT INTO t_rf_b SELECT number * 3, 4 FROM numbers(5000);

SELECT 'large: reverse order';
SELECT if(explain LIKE '%ReadType: InReverseOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1 SELECT * FROM t_rf_merge FINAL ORDER BY x DESC LIMIT 5
) WHERE explain LIKE '%ReadType%';
SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC LIMIT 5;
SELECT count(), sum(x), sum(y) FROM (SELECT * FROM t_rf_merge FINAL ORDER BY x DESC LIMIT 1000);

SELECT 'large: same result without the optimization';
SELECT (SELECT groupArray((x, y)) FROM (SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC LIMIT 1000))
     = (SELECT groupArray((x, y)) FROM (SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC LIMIT 1000 SETTINGS optimize_read_in_order = 0));
SELECT (SELECT groupArray((x, y)) FROM (SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC))
     = (SELECT groupArray((x, y)) FROM (SELECT * FROM t_rf_merge FINAL ORDER BY x DESC, y DESC SETTINGS optimize_read_in_order = 0));

DROP TABLE t_rf_merge;
DROP TABLE t_rf_b;
DROP TABLE t_rf_a;
