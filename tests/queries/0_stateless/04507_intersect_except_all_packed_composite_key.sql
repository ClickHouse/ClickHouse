-- Test INTERSECT ALL / EXCEPT ALL over a composite fixed key that fits in <=4 (keys32)
-- and <=8 (keys64) bytes. ALL operators use the CountingSetVariants (counting multiset)
-- path, so this exercises the packed keys32/keys64 methods with occurrence counting.

DROP TABLE IF EXISTS iea_l16;
DROP TABLE IF EXISTS iea_r16;
DROP TABLE IF EXISTS iea_l32;
DROP TABLE IF EXISTS iea_r32;

-- keys32: two UInt16 columns (4 bytes).
CREATE TABLE iea_l16 (a UInt16, b UInt16) ENGINE = Memory;
CREATE TABLE iea_r16 (a UInt16, b UInt16) ENGINE = Memory;
INSERT INTO iea_l16 VALUES (1,1),(1,1),(1,1),(2,2),(3,3),(3,3);
INSERT INTO iea_r16 VALUES (1,1),(2,2),(2,2),(4,4);

SELECT 'intersect_all_keys32';
SELECT a, b FROM ((SELECT a, b FROM iea_l16) INTERSECT ALL (SELECT a, b FROM iea_r16)) ORDER BY a, b;
SELECT 'except_all_keys32';
SELECT a, b FROM ((SELECT a, b FROM iea_l16) EXCEPT ALL (SELECT a, b FROM iea_r16)) ORDER BY a, b;

-- keys64: two UInt32 columns (8 bytes).
CREATE TABLE iea_l32 (a UInt32, b UInt32) ENGINE = Memory;
CREATE TABLE iea_r32 (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO iea_l32 VALUES (1,1),(1,1),(1,1),(2,2),(3,3),(3,3);
INSERT INTO iea_r32 VALUES (1,1),(2,2),(2,2),(4,4);

SELECT 'intersect_all_keys64';
SELECT a, b FROM ((SELECT a, b FROM iea_l32) INTERSECT ALL (SELECT a, b FROM iea_r32)) ORDER BY a, b;
SELECT 'except_all_keys64';
SELECT a, b FROM ((SELECT a, b FROM iea_l32) EXCEPT ALL (SELECT a, b FROM iea_r32)) ORDER BY a, b;

DROP TABLE iea_l16;
DROP TABLE iea_r16;
DROP TABLE iea_l32;
DROP TABLE iea_r32;
