
SELECT '----- NULL value -----';

SELECT NULL;
SELECT 1 + NULL;
SELECT abs(NULL);
SELECT NULL + NULL;

SELECT '----- MergeTree engine -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = MergeTree(d, (col1, d), 8192);

INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;


SELECT '----- Memory engine -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = Memory;

INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;

SELECT '----- TinyLog engine -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = TinyLog;

INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;

SELECT '----- Log engine -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = Log;

INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;

SELECT '----- StripeLog engine -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(
col1 UInt64, col2 Nullable(UInt64),
col3 String, col4 Nullable(String),
col5 Array(UInt64), col6 Array(Nullable(UInt64)),
col7 Array(String), col8 Array(Nullable(String)),
d Date) Engine = StripeLog;

INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1 VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;


SELECT '----- Insert with expression -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(UInt64))) Engine=Memory;
INSERT INTO test1(col1) VALUES ([1+1]);
SELECT col1 FROM test1 ORDER BY col1 ASC;

SELECT '----- Insert. Source and target columns have same types up to nullability. -----';
DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(UInt64), col2 UInt64) Engine=Memory;
DROP TABLE IF EXISTS test2;
CREATE TABLE test2(col1 UInt64, col2 Nullable(UInt64)) Engine=Memory;
INSERT INTO test1(col1,col2) VALUES (2,7)(6,9)(5,1)(4,3)(8,2);
INSERT INTO test2(col1,col2) SELECT col1,col2 FROM test1;
SELECT col1,col2 FROM test2 ORDER BY col1,col2 ASC;

SELECT '----- Apply functions and aggregate functions on columns that may contain null values -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(UInt64), col2 Nullable(UInt64)) Engine=Memory;
INSERT INTO test1(col1,col2) VALUES (2,7)(NULL,6)(9,NULL)(NULL,NULL)(5,1)(42,42);
SELECT col1, col2, col1 + col2, col1 * 7 FROM test1 ORDER BY col1,col2 ASC;
SELECT sum(col1) FROM test1;
SELECT sum(col1 * 7) FROM test1;

SELECT '----- isNull, isNotNull -----';

SELECT col1, col2, isNull(col1), isNotNull(col2) FROM test1 ORDER BY col1,col2 ASC;

SELECT '----- ifNull, nullIf -----';

SELECT col1, col2, ifNull(col1,col2) FROM test1 ORDER BY col1,col2 ASC;
SELECT col1, col2, nullIf(col1,col2) FROM test1 ORDER BY col1,col2 ASC;

SELECT '----- coalesce -----';

SELECT coalesce(NULL);
SELECT coalesce(NULL, 1);
SELECT coalesce(NULL, NULL, 1);
SELECT coalesce(NULL, 42, NULL, 1);
SELECT coalesce(NULL, NULL, NULL);
SELECT col1, col2, coalesce(col1, col2) FROM test1 ORDER BY col1, col2 ASC;
SELECT col1, col2, coalesce(col1, col2, 99) FROM test1 ORDER BY col1, col2 ASC;

SELECT '----- assumeNotNull -----';

SELECT res FROM (SELECT col1, assumeNotNull(col1) AS res FROM test1) WHERE col1 IS NOT NULL ORDER BY res ASC;

SELECT '----- IS NULL, IS NOT NULL -----';

SELECT col1 FROM test1 WHERE col1 IS NOT NULL ORDER BY col1 ASC;
SELECT col1 FROM test1 WHERE col1 IS NULL;

SELECT '----- if -----';

DROP TABLE IF EXISTS test.test1;
CREATE TABLE test.test1 (col1 Nullable(String)) ENGINE=TinyLog;
INSERT INTO test.test1 VALUES ('a'), ('b'), ('c'), (NULL);

SELECT col1, if(col1 IN ('a' ,'b'), 1, 0) AS t, toTypeName(t) FROM test.test1;
SELECT col1, if(col1 IN ('a' ,'b'), NULL, 0) AS t, toTypeName(t) FROM test.test1;

SELECT '----- case when -----';

SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN 1 ELSE 0 END AS t, toTypeName(t) FROM test.test1;
SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN NULL ELSE 0 END AS t, toTypeName(t) FROM test.test1;
SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN 1 END AS t, toTypeName(t) FROM test.test1;

SELECT '----- multiIf -----';

SELECT multiIf(1, NULL, 1, 3, 4);
SELECT multiIf(1, 2, 1, NULL, 4);
SELECT multiIf(NULL, NULL, NULL);

SELECT multiIf(1, 'A', 1, NULL, 'DEF');
SELECT multiIf(1, toFixedString('A', 16), 1, NULL, toFixedString('DEF', 16));

SELECT multiIf(NULL, 2, 1, 3, 4);
SELECT multiIf(1, 2, NULL, 3, 4);

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(Int8), col2 Nullable(UInt16), col3 Nullable(Float32)) Engine=TinyLog;
INSERT INTO test1(col1,col2,col3) VALUES (toInt8(1),toUInt16(2),toFloat32(3))(NULL,toUInt16(1),toFloat32(2))(toInt8(1),NULL,toFloat32(2))(toInt8(1),toUInt16(2),NULL);
SELECT multiIf(col1 == 1, col2, col2 == 2, col3, col3 == 3, col1, 42) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(cond1 Nullable(UInt8), then1 Int8, cond2 UInt8, then2 Nullable(UInt16), then3 Nullable(Float32)) Engine=TinyLog;
INSERT INTO test1(cond1,then1,cond2,then2,then3) VALUES(1,1,1,42,99)(0,7,1,99,42)(NULL,6,2,99,NULL);
SELECT multiIf(cond1,then1,cond2,then2,then3) FROM test1;

SELECT '----- Array functions -----';

SELECT [NULL];
SELECT [NULL,NULL,NULL];
SELECT [NULL,2,3];
SELECT [1,NULL,3];
SELECT [1,2,NULL];

SELECT [NULL,'b','c'];
SELECT ['a',NULL,'c'];
SELECT ['a','b',NULL];

SELECT '----- arrayElement -----';

SELECT '----- constant arrays -----';

SELECT arrayElement([1,NULL,2,3], 1);
SELECT arrayElement([1,NULL,2,3], 2);
SELECT arrayElement([1,NULL,2,3], 3);
SELECT arrayElement([1,NULL,2,3], 4);

SELECT arrayElement(['a',NULL,'c','d'], 1);
SELECT arrayElement(['a',NULL,'c','d'], 2);
SELECT arrayElement(['a',NULL,'c','d'], 3);
SELECT arrayElement(['a',NULL,'c','d'], 4);

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 UInt64) Engine=TinyLog;
INSERT INTO test1(col1) VALUES(1),(2),(3),(4);

SELECT arrayElement([1,NULL,2,3], col1) FROM test1;

SELECT '----- variable arrays -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(UInt64))) Engine=TinyLog;
INSERT INTO test1(col1) VALUES([2,3,7,NULL]);
INSERT INTO test1(col1) VALUES([NULL,3,7,4]);
INSERT INTO test1(col1) VALUES([2,NULL,7,NULL]);
INSERT INTO test1(col1) VALUES([2,3,NULL,4]);
INSERT INTO test1(col1) VALUES([NULL,NULL,NULL,NULL]);

SELECT arrayElement(col1, 1) FROM test1;
SELECT arrayElement(col1, 2) FROM test1;
SELECT arrayElement(col1, 3) FROM test1;
SELECT arrayElement(col1, 4) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(String))) Engine=TinyLog;
INSERT INTO test1(col1) VALUES(['a','bc','def',NULL]);
INSERT INTO test1(col1) VALUES([NULL,'bc','def','ghij']);
INSERT INTO test1(col1) VALUES(['a',NULL,'def',NULL]);
INSERT INTO test1(col1) VALUES(['a','bc',NULL,'ghij']);
INSERT INTO test1(col1) VALUES([NULL,NULL,NULL,NULL]);

SELECT arrayElement(col1, 1) FROM test1;
SELECT arrayElement(col1, 2) FROM test1;
SELECT arrayElement(col1, 3) FROM test1;
SELECT arrayElement(col1, 4) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(UInt64)), col2 UInt64) Engine=TinyLog;
INSERT INTO test1(col1,col2) VALUES([2,3,7,NULL], 1);
INSERT INTO test1(col1,col2) VALUES([NULL,3,7,4], 2);
INSERT INTO test1(col1,col2) VALUES([2,NULL,7,NULL], 3);
INSERT INTO test1(col1,col2) VALUES([2,3,NULL,4],4);
INSERT INTO test1(col1,col2) VALUES([NULL,NULL,NULL,NULL],3);

SELECT arrayElement(col1,col2) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(String)), col2 UInt64) Engine=TinyLog;
INSERT INTO test1(col1,col2) VALUES(['a','bc','def',NULL], 1);
INSERT INTO test1(col1,col2) VALUES([NULL,'bc','def','ghij'], 2);
INSERT INTO test1(col1,col2) VALUES(['a',NULL,'def','ghij'], 3);
INSERT INTO test1(col1,col2) VALUES(['a','bc',NULL,'ghij'],4);
INSERT INTO test1(col1,col2) VALUES([NULL,NULL,NULL,NULL],3);

SELECT arrayElement(col1,col2) FROM test1;

SELECT '----- has -----';

SELECT '----- constant arrays -----';

SELECT has([1,NULL,2,3], 1);
SELECT has([1,NULL,2,3], NULL);
SELECT has([1,NULL,2,3], 2);
SELECT has([1,NULL,2,3], 3);
SELECT has([1,NULL,2,3], 4);

SELECT has(['a',NULL,'def','ghij'], 'a');
SELECT has(['a',NULL,'def','ghij'], NULL);
SELECT has(['a',NULL,'def','ghij'], 'def');
SELECT has(['a',NULL,'def','ghij'], 'ghij');

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 UInt64) Engine=TinyLog;
INSERT INTO test1(col1) VALUES(1),(2),(3),(4);

SELECT has([1,NULL,2,3], col1) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(UInt64)) Engine=TinyLog;
INSERT INTO test1(col1) VALUES(1),(2),(3),(4),(NULL);

SELECT has([1,NULL,2,3], col1) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 String) Engine=TinyLog;
INSERT INTO test1(col1) VALUES('a'),('bc'),('def'),('ghij');

SELECT has(['a',NULL,'def','ghij'], col1) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(String)) Engine=TinyLog;
INSERT INTO test1(col1) VALUES('a'),('bc'),('def'),('ghij'),(NULL);

SELECT has(['a',NULL,'def','ghij'], col1) FROM test1;

SELECT '----- variable arrays -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(UInt64))) Engine=TinyLog;
INSERT INTO test1(col1) VALUES([2,3,7,NULL]);
INSERT INTO test1(col1) VALUES([NULL,3,7,4]);
INSERT INTO test1(col1) VALUES([2,NULL,7,NULL]);
INSERT INTO test1(col1) VALUES([2,3,NULL,4]);
INSERT INTO test1(col1) VALUES([NULL,NULL,NULL,NULL]);

SELECT has(col1, 2) FROM test1;
SELECT has(col1, 3) FROM test1;
SELECT has(col1, 4) FROM test1;
SELECT has(col1, 5) FROM test1;
SELECT has(col1, 7) FROM test1;
SELECT has(col1, NULL) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(String))) Engine=TinyLog;
INSERT INTO test1(col1) VALUES(['a','bc','def',NULL]);
INSERT INTO test1(col1) VALUES([NULL,'bc','def','ghij']);
INSERT INTO test1(col1) VALUES(['a',NULL,'def',NULL]);
INSERT INTO test1(col1) VALUES(['a','bc',NULL,'ghij']);
INSERT INTO test1(col1) VALUES([NULL,NULL,NULL,NULL]);

SELECT has(col1, 'a') FROM test1;
SELECT has(col1, 'bc') FROM test1;
SELECT has(col1, 'def') FROM test1;
SELECT has(col1, 'ghij') FROM test1;
SELECT has(col1,  NULL) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(UInt64)), col2 UInt64) Engine=TinyLog;
INSERT INTO test1(col1,col2) VALUES([2,3,7,NULL], 2);
INSERT INTO test1(col1,col2) VALUES([NULL,3,7,4], 3);
INSERT INTO test1(col1,col2) VALUES([2,NULL,7,NULL], 7);
INSERT INTO test1(col1,col2) VALUES([2,3,NULL,4],5);

SELECT has(col1,col2) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(UInt64)), col2 Nullable(UInt64)) Engine=TinyLog;
INSERT INTO test1(col1,col2) VALUES([2,3,7,NULL], 2);
INSERT INTO test1(col1,col2) VALUES([NULL,3,7,4], 3);
INSERT INTO test1(col1,col2) VALUES([2,NULL,7,NULL], 7);
INSERT INTO test1(col1,col2) VALUES([2,3,NULL,4],5);
INSERT INTO test1(col1,col2) VALUES([NULL,NULL,NULL,NULL],NULL);

SELECT has(col1,col2) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(String)), col2 String) Engine=TinyLog;
INSERT INTO test1(col1,col2) VALUES(['a','bc','def',NULL], 'a');
INSERT INTO test1(col1,col2) VALUES([NULL,'bc','def','ghij'], 'bc');
INSERT INTO test1(col1,col2) VALUES(['a',NULL,'def','ghij'], 'def');
INSERT INTO test1(col1,col2) VALUES(['a','bc',NULL,'ghij'], 'ghij');

SELECT has(col1,col2) FROM test1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(String)), col2 Nullable(String)) Engine=TinyLog;
INSERT INTO test1(col1,col2) VALUES(['a','bc','def',NULL], 'a');
INSERT INTO test1(col1,col2) VALUES([NULL,'bc','def','ghij'], 'bc');
INSERT INTO test1(col1,col2) VALUES(['a',NULL,'def','ghij'], 'def');
INSERT INTO test1(col1,col2) VALUES(['a','bc',NULL,'ghij'], 'ghij');
INSERT INTO test1(col1,col2) VALUES([NULL,NULL,NULL,NULL], NULL);

SELECT has(col1,col2) FROM test1;

SELECT '----- Aggregation -----';

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(String), col2 Nullable(UInt8), col3 String) ENGINE=TinyLog;
INSERT INTO test1(col1,col2,col3) VALUES('A', 0, 'ABCDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('A', 0, 'BACDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('A', 1, 'BCADEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('A', 1, 'BCDAEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('B', 1, 'BCDEAFGH');
INSERT INTO test1(col1,col2,col3) VALUES('B', 1, 'BCDEFAGH');
INSERT INTO test1(col1,col2,col3) VALUES('B', 1, 'BCDEFGAH');
INSERT INTO test1(col1,col2,col3) VALUES('B', 1, 'BCDEFGHA');
INSERT INTO test1(col1,col2,col3) VALUES('C', 1, 'ACBDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('C', NULL, 'ACDBEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('C', NULL, 'ACDEBFGH');
INSERT INTO test1(col1,col2,col3) VALUES('C', NULL, 'ACDEFBGH');
INSERT INTO test1(col1,col2,col3) VALUES(NULL, 1, 'ACDEFGBH');
INSERT INTO test1(col1,col2,col3) VALUES(NULL, NULL, 'ACDEFGHB');

SELECT col1, col2, count() FROM test1 GROUP BY col1, col2 ORDER BY col1, col2;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 String, col2 Nullable(UInt8), col3 String) ENGINE=TinyLog;
INSERT INTO test1(col1,col2,col3) VALUES('A', 0, 'ABCDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('A', 0, 'BACDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('A', 1, 'BCADEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('A', 1, 'BCDAEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('B', 1, 'BCDEAFGH');
INSERT INTO test1(col1,col2,col3) VALUES('B', 1, 'BCDEFAGH');
INSERT INTO test1(col1,col2,col3) VALUES('B', 1, 'BCDEFGAH');
INSERT INTO test1(col1,col2,col3) VALUES('B', 1, 'BCDEFGHA');
INSERT INTO test1(col1,col2,col3) VALUES('C', 1, 'ACBDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('C', NULL, 'ACDBEFGH');
INSERT INTO test1(col1,col2,col3) VALUES('C', NULL, 'ACDEBFGH');
INSERT INTO test1(col1,col2,col3) VALUES('C', NULL, 'ACDEFBGH');

SELECT col1, col2, count() FROM test1 GROUP BY col1, col2 ORDER BY col1, col2;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(String), col2 String) ENGINE=TinyLog;
INSERT INTO test1(col1,col2) VALUES('A', 'ABCDEFGH');
INSERT INTO test1(col1,col2) VALUES('A', 'BACDEFGH');
INSERT INTO test1(col1,col2) VALUES('A', 'BCADEFGH');
INSERT INTO test1(col1,col2) VALUES('A', 'BCDAEFGH');
INSERT INTO test1(col1,col2) VALUES('B', 'BCDEAFGH');
INSERT INTO test1(col1,col2) VALUES('B', 'BCDEFAGH');
INSERT INTO test1(col1,col2) VALUES('B', 'BCDEFGAH');
INSERT INTO test1(col1,col2) VALUES('B', 'BCDEFGHA');
INSERT INTO test1(col1,col2) VALUES('C', 'ACBDEFGH');
INSERT INTO test1(col1,col2) VALUES('C', 'ACDBEFGH');
INSERT INTO test1(col1,col2) VALUES('C', 'ACDEBFGH');
INSERT INTO test1(col1,col2) VALUES('C', 'ACDEFBGH');
INSERT INTO test1(col1,col2) VALUES(NULL, 'ACDEFGBH');
INSERT INTO test1(col1,col2) VALUES(NULL, 'ACDEFGHB');

SELECT col1, count() FROM test1 GROUP BY col1 ORDER BY col1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(UInt8), col2 String) ENGINE=TinyLog;
INSERT INTO test1(col1,col2) VALUES(0, 'ABCDEFGH');
INSERT INTO test1(col1,col2) VALUES(0, 'BACDEFGH');
INSERT INTO test1(col1,col2) VALUES(1, 'BCADEFGH');
INSERT INTO test1(col1,col2) VALUES(1, 'BCDAEFGH');
INSERT INTO test1(col1,col2) VALUES(1, 'BCDEAFGH');
INSERT INTO test1(col1,col2) VALUES(1, 'BCDEFAGH');
INSERT INTO test1(col1,col2) VALUES(1, 'BCDEFGAH');
INSERT INTO test1(col1,col2) VALUES(1, 'BCDEFGHA');
INSERT INTO test1(col1,col2) VALUES(1, 'ACBDEFGH');
INSERT INTO test1(col1,col2) VALUES(NULL, 'ACDBEFGH');
INSERT INTO test1(col1,col2) VALUES(NULL, 'ACDEBFGH');
INSERT INTO test1(col1,col2) VALUES(NULL, 'ACDEFBGH');

SELECT col1, count() FROM test1 GROUP BY col1 ORDER BY col1;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(UInt64), col2 UInt64, col3 String) ENGINE=TinyLog;
INSERT INTO test1(col1,col2,col3) VALUES(0, 2, 'ABCDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES(0, 3, 'BACDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES(1, 5, 'BCADEFGH');
INSERT INTO test1(col1,col2,col3) VALUES(1, 2, 'BCDAEFGH');
INSERT INTO test1(col1,col2,col3) VALUES(1, 3, 'BCDEAFGH');
INSERT INTO test1(col1,col2,col3) VALUES(1, 5, 'BCDEFAGH');
INSERT INTO test1(col1,col2,col3) VALUES(1, 2, 'BCDEFGAH');
INSERT INTO test1(col1,col2,col3) VALUES(1, 3, 'BCDEFGHA');
INSERT INTO test1(col1,col2,col3) VALUES(1, 5, 'ACBDEFGH');
INSERT INTO test1(col1,col2,col3) VALUES(NULL, 2, 'ACDBEFGH');
INSERT INTO test1(col1,col2,col3) VALUES(NULL, 3, 'ACDEBFGH');
INSERT INTO test1(col1,col2,col3) VALUES(NULL, 3, 'ACDEFBGH');

SELECT col1, col2, count() FROM test1 GROUP BY col1, col2 ORDER BY col1, col2;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Nullable(UInt64), col2 UInt64, col3 Nullable(UInt64), col4 String) ENGINE=TinyLog;
INSERT INTO test1(col1,col2,col3,col4) VALUES(0, 2, 1, 'ABCDEFGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(0, 3, NULL, 'BACDEFGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(1, 5, 1, 'BCADEFGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(1, 2, NULL, 'BCDAEFGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(1, 3, 1, 'BCDEAFGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(1, 5, NULL, 'BCDEFAGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(1, 2, 1, 'BCDEFGAH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(1, 3, NULL, 'BCDEFGHA');
INSERT INTO test1(col1,col2,col3,col4) VALUES(1, 5, 1, 'ACBDEFGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(NULL, 2, NULL, 'ACDBEFGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(NULL, 3, 1, 'ACDEBFGH');
INSERT INTO test1(col1,col2,col3,col4) VALUES(NULL, 3, NULL, 'ACDEFBGH');

SELECT col1, col2, col3, count() FROM test1 GROUP BY col1, col2, col3 ORDER BY col1, col2, col3;

DROP TABLE IF EXISTS test1;
CREATE TABLE test1(col1 Array(Nullable(UInt8)), col2 String) ENGINE=TinyLog;
INSERT INTO test1(col1,col2) VALUES([0], 'ABCDEFGH');
INSERT INTO test1(col1,col2) VALUES([0], 'BACDEFGH');
INSERT INTO test1(col1,col2) VALUES([1], 'BCADEFGH');
INSERT INTO test1(col1,col2) VALUES([1], 'BCDAEFGH');
INSERT INTO test1(col1,col2) VALUES([1], 'BCDEAFGH');
INSERT INTO test1(col1,col2) VALUES([1], 'BCDEFAGH');
INSERT INTO test1(col1,col2) VALUES([1], 'BCDEFGAH');
INSERT INTO test1(col1,col2) VALUES([1], 'BCDEFGHA');
INSERT INTO test1(col1,col2) VALUES([1], 'ACBDEFGH');
INSERT INTO test1(col1,col2) VALUES([NULL], 'ACDBEFGH');
INSERT INTO test1(col1,col2) VALUES([NULL], 'ACDEBFGH');
INSERT INTO test1(col1,col2) VALUES([NULL], 'ACDEFBGH');

SELECT col1, count() FROM test1 GROUP BY col1 ORDER BY col1;
