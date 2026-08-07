SELECT ' UInt32 | UInt64 ';

DROP TABLE IF EXISTS u32;
DROP TABLE IF EXISTS u64;
DROP TABLE IF EXISTS merge_32_64;

CREATE TABLE u32 (x UInt32, y UInt32 DEFAULT x) ENGINE = Memory;
CREATE TABLE u64 (x UInt64, y UInt64 DEFAULT x) ENGINE = Memory;
CREATE TABLE merge_32_64 (x UInt64) ENGINE = Merge(currentDatabase(), '^u32|u64$');

INSERT INTO u32 (x) VALUES (1);
INSERT INTO u64 (x) VALUES (1);

INSERT INTO u32 (x) VALUES (4294967290);
INSERT INTO u64 (x) VALUES (4294967290);
--now inserts 3. maybe need out of range check?
--INSERT INTO u32 VALUES (4294967299);
INSERT INTO u64 (x) VALUES (4294967299);

select ' = 1:';
SELECT x FROM merge_32_64 WHERE x = 1;
select ' 1:';
SELECT x FROM merge_32_64 WHERE x IN (1);
select ' 4294967290:';
SELECT x FROM merge_32_64 WHERE x IN (4294967290);
select ' 4294967299:';
SELECT x FROM merge_32_64 WHERE x IN (4294967299);
--select ' -1: ';
--SELECT x FROM merge_32_64 WHERE x IN (-1);

DROP TABLE u32;
DROP TABLE u64;
DROP TABLE merge_32_64;


SELECT ' Int64 | UInt64 ';

DROP TABLE IF EXISTS s64;
DROP TABLE IF EXISTS u64;
DROP TABLE IF EXISTS merge_s64_u64;

CREATE TABLE s64 (x Int64) ENGINE = Memory;
CREATE TABLE u64 (x UInt64) ENGINE = Memory;
CREATE TABLE merge_s64_u64 (x UInt64) ENGINE = Merge(currentDatabase(), '^s64|u64$');

INSERT INTO s64 VALUES (1);
INSERT INTO s64 VALUES (-1);
INSERT INTO u64 VALUES (1);

select ' 1:';
SELECT x FROM merge_s64_u64 WHERE x IN (1);
select ' -1: ';
SELECT x FROM merge_s64_u64 WHERE x IN (-1);

DROP TABLE s64;
DROP TABLE u64;
DROP TABLE merge_s64_u64;


SELECT ' Int32 | UInt64 ';

DROP TABLE IF EXISTS one_00458;
DROP TABLE IF EXISTS two_00458;
DROP TABLE IF EXISTS merge_one_two;

CREATE TABLE one_00458 (x Int32) ENGINE = Memory;
CREATE TABLE two_00458 (x UInt64) ENGINE = Memory;
CREATE TABLE merge_one_two (x UInt64) ENGINE = Merge(currentDatabase(), '^one_00458$|^two_00458$');

INSERT INTO one_00458 VALUES (1);
INSERT INTO two_00458 VALUES (1);

INSERT INTO one_00458 VALUES (2147483650);
INSERT INTO two_00458 VALUES (2147483650);

SELECT * FROM merge_one_two WHERE x IN (1);
SELECT x FROM merge_one_two WHERE x IN (2147483650);
SELECT x FROM merge_one_two WHERE x IN (-1);


SELECT ' String | FixedString(16) ';

DROP TABLE IF EXISTS one_00458;
DROP TABLE IF EXISTS two_00458;
DROP TABLE IF EXISTS merge_one_two;

CREATE TABLE one_00458 (x String) ENGINE = Memory;
CREATE TABLE two_00458 (x FixedString(16)) ENGINE = Memory;
CREATE TABLE merge_one_two (x String) ENGINE = Merge(currentDatabase(), '^one_00458$|^two_00458$');

INSERT INTO one_00458 VALUES ('1');
INSERT INTO two_00458 VALUES ('1');

SELECT * FROM merge_one_two WHERE x IN ('1');


SELECT ' DateTime | UInt64 ';

DROP TABLE IF EXISTS one_00458;
DROP TABLE IF EXISTS two_00458;
DROP TABLE IF EXISTS merge_one_two;

CREATE TABLE one_00458 (x DateTime) ENGINE = Memory;
CREATE TABLE two_00458 (x UInt64) ENGINE = Memory;
CREATE TABLE merge_one_two (x UInt64) ENGINE = Merge(currentDatabase(), '^one_00458$|^two_00458$');

INSERT INTO one_00458 VALUES (1);
INSERT INTO two_00458 VALUES (1);

SELECT * FROM merge_one_two WHERE x IN (1);


SELECT '  Array(UInt32) | Array(UInt64) ';

DROP TABLE IF EXISTS one_00458;
DROP TABLE IF EXISTS two_00458;
DROP TABLE IF EXISTS merge_one_two;

CREATE TABLE one_00458 (x Array(UInt32), z String DEFAULT '', y Array(UInt32)) ENGINE = Memory;
CREATE TABLE two_00458 (x Array(UInt64), z String DEFAULT '', y Array(UInt64)) ENGINE = Memory;
CREATE TABLE merge_one_two (x Array(UInt64), z String, y Array(UInt64)) ENGINE = Merge(currentDatabase(), '^one_00458$|^two_00458$');

INSERT INTO one_00458 (x, y) VALUES ([1], [0]);
INSERT INTO two_00458 (x, y) VALUES ([1], [0]);
INSERT INTO one_00458 (x, y) VALUES ([4294967290], [4294967290]);
INSERT INTO two_00458 (x, y) VALUES ([4294967290], [4294967290]);
INSERT INTO one_00458 (x, y) VALUES ([4294967299], [4294967299]);
INSERT INTO two_00458 (x, y) VALUES ([4294967299], [4294967299]);

SELECT x, y FROM merge_one_two WHERE arrayExists(_ -> _ IN (1), x);
SELECT x, y FROM merge_one_two WHERE arrayExists(_ -> _ IN (4294967290), x);
SELECT x, y FROM merge_one_two WHERE arrayExists(_ -> _ IN (4294967299), x);

DROP TABLE IF EXISTS one_00458;
DROP TABLE IF EXISTS two_00458;
DROP TABLE IF EXISTS merge_one_two;
