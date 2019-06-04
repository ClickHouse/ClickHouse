SELECT ' UInt32 | UInt64 ';

DROP TABLE IF EXISTS test.u32;
DROP TABLE IF EXISTS test.u64;
DROP TABLE IF EXISTS test.merge_32_64;

CREATE TABLE test.u32 (x UInt32, y UInt32 DEFAULT x) ENGINE = Memory;
CREATE TABLE test.u64 (x UInt64, y UInt64 DEFAULT x) ENGINE = Memory;
CREATE TABLE test.merge_32_64 (x UInt64) ENGINE = Merge(test, '^u32|u64$');

INSERT INTO test.u32 (x) VALUES (1);
INSERT INTO test.u64 (x) VALUES (1);

INSERT INTO test.u32 (x) VALUES (4294967290);
INSERT INTO test.u64 (x) VALUES (4294967290);
--now inserts 3. maybe need out of range check?
--INSERT INTO test.u32 VALUES (4294967299);
INSERT INTO test.u64 (x) VALUES (4294967299);

select ' = 1:';
SELECT x FROM test.merge_32_64 WHERE x = 1;
select ' 1:';
SELECT x FROM test.merge_32_64 WHERE x IN (1);
select ' 4294967290:';
SELECT x FROM test.merge_32_64 WHERE x IN (4294967290);
select ' 4294967299:';
SELECT x FROM test.merge_32_64 WHERE x IN (4294967299);
--select ' -1: ';
--SELECT x FROM test.merge_32_64 WHERE x IN (-1);

DROP TABLE test.u32;
DROP TABLE test.u64;
DROP TABLE test.merge_32_64;


SELECT ' Int64 | UInt64 ';

DROP TABLE IF EXISTS test.s64;
DROP TABLE IF EXISTS test.u64;
DROP TABLE IF EXISTS test.merge_s64_u64;

CREATE TABLE test.s64 (x Int64) ENGINE = Memory;
CREATE TABLE test.u64 (x UInt64) ENGINE = Memory;
CREATE TABLE test.merge_s64_u64 (x UInt64) ENGINE = Merge(test, '^s64|u64$');

INSERT INTO test.s64 VALUES (1);
INSERT INTO test.s64 VALUES (-1);
INSERT INTO test.u64 VALUES (1);

select ' 1:';
SELECT x FROM test.merge_s64_u64 WHERE x IN (1);
select ' -1: ';
SELECT x FROM test.merge_s64_u64 WHERE x IN (-1);

DROP TABLE test.s64;
DROP TABLE test.u64;
DROP TABLE test.merge_s64_u64;


SELECT ' Int32 | UInt64 ';

DROP TABLE IF EXISTS test.one;
DROP TABLE IF EXISTS test.two;
DROP TABLE IF EXISTS test.merge_one_two;

CREATE TABLE test.one (x Int32) ENGINE = Memory;
CREATE TABLE test.two (x UInt64) ENGINE = Memory;
CREATE TABLE test.merge_one_two (x UInt64) ENGINE = Merge(test, '^one$|^two$');

INSERT INTO test.one VALUES (1);
INSERT INTO test.two VALUES (1);

INSERT INTO test.one VALUES (2147483650);
INSERT INTO test.two VALUES (2147483650);

SELECT * FROM test.merge_one_two WHERE x IN (1);
SELECT x FROM test.merge_one_two WHERE x IN (2147483650);
SELECT x FROM test.merge_one_two WHERE x IN (-1);


SELECT ' String | FixedString(16) ';

DROP TABLE IF EXISTS test.one;
DROP TABLE IF EXISTS test.two;
DROP TABLE IF EXISTS test.merge_one_two;

CREATE TABLE test.one (x String) ENGINE = Memory;
CREATE TABLE test.two (x FixedString(16)) ENGINE = Memory;
CREATE TABLE test.merge_one_two (x String) ENGINE = Merge(test, '^one$|^two$');

INSERT INTO test.one VALUES ('1');
INSERT INTO test.two VALUES ('1');

SELECT * FROM test.merge_one_two WHERE x IN ('1');


SELECT ' DateTime | UInt64 ';

DROP TABLE IF EXISTS test.one;
DROP TABLE IF EXISTS test.two;
DROP TABLE IF EXISTS test.merge_one_two;

CREATE TABLE test.one (x DateTime) ENGINE = Memory;
CREATE TABLE test.two (x UInt64) ENGINE = Memory;
CREATE TABLE test.merge_one_two (x UInt64) ENGINE = Merge(test, '^one$|^two$');

INSERT INTO test.one VALUES (1);
INSERT INTO test.two VALUES (1);

SELECT * FROM test.merge_one_two WHERE x IN (1);


SELECT '  Array(UInt32) | Array(UInt64) ';

DROP TABLE IF EXISTS test.one;
DROP TABLE IF EXISTS test.two;
DROP TABLE IF EXISTS test.merge_one_two;

CREATE TABLE test.one (x Array(UInt32), z String DEFAULT '', y Array(UInt32)) ENGINE = Memory;
CREATE TABLE test.two (x Array(UInt64), z String DEFAULT '', y Array(UInt64)) ENGINE = Memory;
CREATE TABLE test.merge_one_two (x Array(UInt64), z String, y Array(UInt64)) ENGINE = Merge(test, '^one$|^two$');

INSERT INTO test.one (x, y) VALUES ([1], [0]);
INSERT INTO test.two (x, y) VALUES ([1], [0]);
INSERT INTO test.one (x, y) VALUES ([4294967290], [4294967290]);
INSERT INTO test.two (x, y) VALUES ([4294967290], [4294967290]);
INSERT INTO test.one (x, y) VALUES ([4294967299], [4294967299]);
INSERT INTO test.two (x, y) VALUES ([4294967299], [4294967299]);

SELECT x, y FROM test.merge_one_two WHERE arrayExists(_ -> _ IN (1), x);
SELECT x, y FROM test.merge_one_two WHERE arrayExists(_ -> _ IN (4294967290), x);
SELECT x, y FROM test.merge_one_two WHERE arrayExists(_ -> _ IN (4294967299), x);

DROP TABLE IF EXISTS test.one;
DROP TABLE IF EXISTS test.two;
DROP TABLE IF EXISTS test.merge_one_two;
