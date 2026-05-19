-- Test: exercises `ColumnDecimal::compareTrackAt` res > 0 branch at ColumnDecimal.cpp:100-103
-- Covers: src/Columns/ColumnDecimal.cpp:100-103 — res > 0 tracking branch in compareTrackAt for merge join
DROP TABLE IF EXISTS t1_dec_pm;
DROP TABLE IF EXISTS t2_dec_pm;
CREATE TABLE t1_dec_pm (id Decimal32(0), val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2_dec_pm (id Decimal32(0), val String) ENGINE = MergeTree ORDER BY id;
-- Data triggers res > 0: t1[0]=5 vs t2[0]=1 → 5 > 1, res=+1
-- Tracking loop: t2[1]=2 < 5 (res=2), t2[2]=3 < 5 (res=3), t2[3]=5 < 5 (no, exit)
INSERT INTO t1_dec_pm VALUES (5, 'A'), (6, 'B'), (7, 'C');
INSERT INTO t2_dec_pm VALUES (1, 'X'), (2, 'Y'), (3, 'Z'), (5, 'AA'), (6, 'BB');
SET join_algorithm = 'partial_merge';
SELECT t1.id, t1.val, t2.id, t2.val FROM t1_dec_pm t1 INNER JOIN t2_dec_pm t2 ON t1.id = t2.id ORDER BY t1.id;
DROP TABLE IF EXISTS t3_dec_pm;
DROP TABLE IF EXISTS t4_dec_pm;
CREATE TABLE t3_dec_pm (id Decimal32(2), val Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t4_dec_pm (id Decimal32(2), val Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t3_dec_pm VALUES (10.00, 100), (11.00, 110), (12.00, 120);
INSERT INTO t4_dec_pm VALUES (1.00, 1), (2.00, 2), (10.00, 10), (11.00, 11);
SELECT t3.id, t3.val, t4.id, t4.val FROM t3_dec_pm t3 INNER JOIN t4_dec_pm t4 ON t3.id = t4.id ORDER BY t3.id;
DROP TABLE t1_dec_pm;
DROP TABLE t2_dec_pm;
DROP TABLE t3_dec_pm;
DROP TABLE t4_dec_pm;
