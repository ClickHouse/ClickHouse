SET experimental_allow_extended_storage_definition_syntax = 1;

SELECT '*** Not partitioned ***';

DROP TABLE IF EXISTS test.not_partitioned;
CREATE TABLE test.not_partitioned(x UInt8) ENGINE MergeTree ORDER BY x;

INSERT INTO test.not_partitioned VALUES (1), (2), (3);
INSERT INTO test.not_partitioned VALUES (4), (5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = 'test' AND table = 'not_partitioned' AND active ORDER BY name;
OPTIMIZE TABLE test.not_partitioned PARTITION tuple() FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = 'test' AND table = 'not_partitioned' AND active ORDER BY name;

SELECT 'Sum before DETACH PARTITION:';
SELECT sum(x) FROM test.not_partitioned;
ALTER TABLE test.not_partitioned DETACH PARTITION ID 'all';
SELECT 'Sum after DETACH PARTITION:';
SELECT sum(x) FROM test.not_partitioned;

DROP TABLE test.not_partitioned;

SELECT '*** Partitioned by week ***';

DROP TABLE IF EXISTS test.partitioned_by_week;
CREATE TABLE test.partitioned_by_week(d Date, x UInt8) ENGINE = MergeTree PARTITION BY toMonday(d) ORDER BY x;

-- 2000-01-03 belongs to a different week than 2000-01-01 and 2000-01-02
INSERT INTO test.partitioned_by_week VALUES ('2000-01-01', 1), ('2000-01-02', 2), ('2000-01-03', 3);
INSERT INTO test.partitioned_by_week VALUES ('2000-01-03', 4), ('2000-01-03', 5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = 'test' AND table = 'partitioned_by_week' AND active ORDER BY name;
OPTIMIZE TABLE test.partitioned_by_week PARTITION '2000-01-03' FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = 'test' AND table = 'partitioned_by_week' AND active ORDER BY name;

SELECT 'Sum before DROP PARTITION:';
SELECT sum(x) FROM test.partitioned_by_week;
ALTER TABLE test.partitioned_by_week DROP PARTITION '1999-12-27';
SELECT 'Sum after DROP PARTITION:';
SELECT sum(x) FROM test.partitioned_by_week;

DROP TABLE test.partitioned_by_week;

SELECT '*** Partitioned by a (Date, UInt8) tuple ***';

DROP TABLE IF EXISTS test.partitioned_by_tuple;
CREATE TABLE test.partitioned_by_tuple(d Date, x UInt8, y UInt8) ENGINE MergeTree ORDER BY x PARTITION BY (d, x);

INSERT INTO test.partitioned_by_tuple VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2), ('2000-01-02', 1, 3);
INSERT INTO test.partitioned_by_tuple VALUES ('2000-01-02', 1, 4), ('2000-01-01', 1, 5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = 'test' AND table = 'partitioned_by_tuple' AND active ORDER BY name;
OPTIMIZE TABLE test.partitioned_by_tuple PARTITION ('2000-01-01', 1) FINAL;
OPTIMIZE TABLE test.partitioned_by_tuple PARTITION ('2000-01-02', 1) FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = 'test' AND table = 'partitioned_by_tuple' AND active ORDER BY name;

SELECT 'Sum before DETACH PARTITION:';
SELECT sum(y) FROM test.partitioned_by_tuple;
ALTER TABLE test.partitioned_by_tuple DETACH PARTITION ID '20000101-1';
SELECT 'Sum after DETACH PARTITION:';
SELECT sum(y) FROM test.partitioned_by_tuple;

DROP TABLE test.partitioned_by_tuple;

SELECT '*** Partitioned by String ***';

DROP TABLE IF EXISTS test.partitioned_by_string;
CREATE TABLE test.partitioned_by_string(s String, x UInt8) ENGINE = MergeTree PARTITION BY s ORDER BY x;

INSERT INTO test.partitioned_by_string VALUES ('aaa', 1), ('aaa', 2), ('bbb', 3);
INSERT INTO test.partitioned_by_string VALUES ('bbb', 4), ('aaa', 5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = 'test' AND table = 'partitioned_by_string' AND active ORDER BY name;
OPTIMIZE TABLE test.partitioned_by_string PARTITION 'aaa' FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = 'test' AND table = 'partitioned_by_string' AND active ORDER BY name;

SELECT 'Sum before DROP PARTITION:';
SELECT sum(x) FROM test.partitioned_by_string;
ALTER TABLE test.partitioned_by_string DROP PARTITION 'bbb';
SELECT 'Sum after DROP PARTITION:';
SELECT sum(x) FROM test.partitioned_by_string;

DROP TABLE test.partitioned_by_string;
