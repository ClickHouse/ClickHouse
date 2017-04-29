DROP TABLE IF EXISTS test.u32;
DROP TABLE IF EXISTS test.u64;
DROP TABLE IF EXISTS test.merge_32_64;

CREATE TABLE test.u32 (x UInt32) ENGINE = Memory;
CREATE TABLE test.u64 (x UInt64) ENGINE = Memory;
CREATE TABLE test.merge_32_64 (x UInt64) ENGINE = Merge(test, 'u32|u64');

INSERT INTO test.u32 VALUES (1);
INSERT INTO test.u64 VALUES (1);

INSERT INTO test.u32 VALUES (4294967290);
INSERT INTO test.u64 VALUES (4294967290);
--now inserts 3. maybe need out of range check?
--INSERT INTO test.u32 VALUES (4294967299);
INSERT INTO test.u64 VALUES (4294967299);

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



DROP TABLE IF EXISTS test.s64;
DROP TABLE IF EXISTS test.u64;
DROP TABLE IF EXISTS test.merge_s64_u64;

CREATE TABLE test.s64 (x Int64) ENGINE = Memory;
CREATE TABLE test.u64 (x UInt64) ENGINE = Memory;
CREATE TABLE test.merge_s64_u64 (x UInt64) ENGINE = Merge(test, 's64|u64');

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
