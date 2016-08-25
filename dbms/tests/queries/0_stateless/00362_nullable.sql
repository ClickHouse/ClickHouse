
/* NULL value */

SELECT NULL;
SELECT 1 + NULL;
SELECT abs(NULL);
SELECT NULL + NULL;

/* Memory engine */

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(
col1 Nullable(UInt64), col2 UInt64,
col3 Nullable(Array(UInt64)), col4 Array(UInt64),
col5 Nullable(String), col6 String,
col7 Nullable(Array(String)), col8 Array(String),
col9 Nullable(FixedString(3)), col10 FixedString(3),
col11 Nullable(Array(FixedString(3))), col12 Array(FixedString(3))) Engine = Memory;

INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (NULL, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, NULL, [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], NULL, 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', NULL, ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], NULL, toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), NULL, [toFixedString('a',3)]);
SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 FROM test.test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12 ASC;

/* TinyLog engine */

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(
col1 Nullable(UInt64), col2 UInt64,
col3 Nullable(Array(UInt64)), col4 Array(UInt64),
col5 Nullable(String), col6 String,
col7 Nullable(Array(String)), col8 Array(String),
col9 Nullable(FixedString(3)), col10 FixedString(3),
col11 Nullable(Array(FixedString(3))), col12 Array(FixedString(3))) Engine = TinyLog;

INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (NULL, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, NULL, [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], NULL, 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', NULL, ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], NULL, toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), NULL, [toFixedString('a',3)]);
SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 FROM test.test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12 ASC;

/* Log engine */

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(
col1 Nullable(UInt64), col2 UInt64,
col3 Nullable(Array(UInt64)), col4 Array(UInt64),
col5 Nullable(String), col6 String,
col7 Nullable(Array(String)), col8 Array(String),
col9 Nullable(FixedString(3)), col10 FixedString(3),
col11 Nullable(Array(FixedString(3))), col12 Array(FixedString(3))) Engine = Log;

INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (NULL, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, NULL, [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], NULL, 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', NULL, ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], NULL, toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)]);
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), NULL, [toFixedString('a',3)]);
SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 FROM test.test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12 ASC;

/* MergeTree engine */

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(
col1 Nullable(UInt64), col2 UInt64,
col3 Nullable(Array(UInt64)), col4 Array(UInt64),
col5 Nullable(String), col6 String,
col7 Nullable(Array(String)), col8 Array(String),
col9 Nullable(FixedString(3)), col10 FixedString(3),
col11 Nullable(Array(FixedString(3))), col12 Array(FixedString(3)),
col13 Date) Engine = MergeTree(col13, (col2, col13), 8192);

INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)], '1970-01-01');
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) VALUES (NULL, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)], '1970-01-01');
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) VALUES (1, 1, NULL, [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)], '1970-01-01');
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) VALUES (1, 1, [1], [1], NULL, 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)], '1970-01-01');
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) VALUES (1, 1, [1], [1], 'a', 'a', NULL, ['a'], toFixedString('a', 3), toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)], '1970-01-01');
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], NULL, toFixedString('a', 3), [toFixedString('a',3)], [toFixedString('a',3)], '1970-01-01');
INSERT INTO test.test1(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) VALUES (1, 1, [1], [1], 'a', 'a', ['a'], ['a'], toFixedString('a', 3), toFixedString('a', 3), NULL, [toFixedString('a',3)], '1970-01-01');
SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13 FROM test.test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12 ASC;

/* Insert with expression */

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(col1 Nullable(Array(UInt64))) Engine=Memory;
INSERT INTO test.test1(col1) VALUES ([1+1]);
SELECT col1 FROM test.test1 ORDER BY col1 ASC;

/* Insert. Source and target columns have same types up to nullability. */
DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(col1 Nullable(UInt64), col2 UInt64) Engine=Memory;
DROP TABLE IF EXISTS test.test2;
CREATE TABLE test.test2(col1 UInt64, col2 Nullable(UInt64)) Engine=Memory;
INSERT INTO test.test1(col1,col2) VALUES (2,7)(6,9)(5,1)(4,3)(8,2);
INSERT INTO test.test2(col1,col2) SELECT col1,col2 FROM test.test1;
SELECT col1,col2 FROM test.test2 ORDER BY col1,col2 ASC;

/* Apply functions and aggregate functions on columns that may contain null values */

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(col1 Nullable(UInt64), col2 Nullable(UInt64)) Engine=Memory;
INSERT INTO test.test1(col1,col2) VALUES (2,7)(NULL,6)(9,NULL)(NULL,NULL)(5,1)(42,42);
SELECT col1, col2, col1 + col2, col1 * 7 FROM test.test1 ORDER BY col1,col2 ASC;
SELECT sum(col1) FROM test.test1;
SELECT sum(col1 * 7) FROM test.test1;

/* isNull, isNotNull */

SELECT col1, col2, isNull(col1), isNotNull(col2) FROM test.test1 ORDER BY col1,col2 ASC;

/* ifNull, nullIf */

SELECT col1, col2, ifNull(col1,col2) FROM test.test1 ORDER BY col1,col2 ASC;
SELECT col1, col2, nullIf(col1,col2) FROM test.test1 ORDER BY col1,col2 ASC;

/* coalesce */

SELECT coalesce(NULL);
SELECT coalesce(NULL, 1);
SELECT coalesce(NULL, NULL, 1);
SELECT coalesce(NULL, 42, NULL, 1);
SELECT coalesce(NULL, NULL, NULL);
SELECT col1, col2, coalesce(col1, col2) FROM test.test1 ORDER BY col1, col2 ASC;
SELECT col1, col2, coalesce(col1, col2, 99) FROM test.test1 ORDER BY col1, col2 ASC;

/* assumeNotNull */

SELECT res FROM (SELECT col1, assumeNotNull(col1) AS res FROM test.test1) WHERE col1 IS NOT NULL ORDER BY res ASC;

/* IS NULL, IS NOT NULL */

SELECT col1 FROM test.test1 WHERE col1 IS NOT NULL ORDER BY col1 ASC;
SELECT col1 FROM test.test1 WHERE col1 IS NULL;

/* multiIf */

SELECT multiIf(1, NULL, 1, 3, 4);
SELECT multiIf(1, 2, 1, NULL, 4);
SELECT multiIf(NULL, NULL, NULL);

SELECT multiIf(1, 'A', 1, NULL, 'DEF');
SELECT multiIf(1, toFixedString('A', 16), 1, NULL, toFixedString('DEF', 16));
SELECT multiIf(1, [1,2], 1, NULL, [5,6]);
SELECT multiIf(1, ['A', 'B'], 1, NULL, ['E', 'F']);

SELECT multiIf(NULL, 2, 1, 3, 4);
SELECT multiIf(1, 2, NULL, 3, 4);

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(col1 Nullable(Int8), col2 Nullable(UInt16), col3 Nullable(Float32)) Engine=TinyLog;
INSERT INTO test.test1(col1,col2,col3) VALUES (toInt8(1),toUInt16(2),toFloat32(3))(NULL,toUInt16(1),toFloat32(2))(toInt8(1),NULL,toFloat32(2))(toInt8(1),toUInt16(2),NULL);
SELECT multiIf(col1 == 1, col2, col2 == 2, col3, col3 == 3, col1, 42) FROM test.test1;

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1(cond1 Nullable(UInt8), then1 Int8, cond2 UInt8, then2 Nullable(UInt16), then3 Nullable(Float32)) Engine=TinyLog;
INSERT INTO test.test1(cond1,then1,cond2,then2,then3) VALUES(1,1,1,42,99)(0,7,1,99,42)(NULL,6,2,99,NULL);
SELECT multiIf(cond1,then1,cond2,then2,then3) FROM test.test1;
