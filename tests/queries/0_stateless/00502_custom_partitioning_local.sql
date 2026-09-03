SELECT '*** Not partitioned ***';

DROP TABLE IF EXISTS not_partitioned;
CREATE TABLE not_partitioned(x UInt8) ENGINE MergeTree ORDER BY x;

INSERT INTO not_partitioned VALUES (1), (2), (3);
INSERT INTO not_partitioned VALUES (4), (5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'not_partitioned' AND active ORDER BY name;
OPTIMIZE TABLE not_partitioned PARTITION tuple() FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'not_partitioned' AND active ORDER BY name;

SELECT 'Sum before DETACH PARTITION:';
SELECT sum(x) FROM not_partitioned;
ALTER TABLE not_partitioned DETACH PARTITION ID 'all';
SELECT 'Sum after DETACH PARTITION:';
SELECT sum(x) FROM not_partitioned;
SELECT 'system.detached_parts after DETACH PARTITION:';
SELECT system.detached_parts.* EXCEPT (bytes_on_disk, `path`, disk, modification_time) FROM system.detached_parts WHERE database = currentDatabase() AND table = 'not_partitioned';

DROP TABLE not_partitioned;

SELECT '*** Partitioned by week ***';

DROP TABLE IF EXISTS partitioned_by_week;
CREATE TABLE partitioned_by_week(d Date, x UInt8) ENGINE = MergeTree PARTITION BY toMonday(d) ORDER BY x;

-- 2000-01-03 belongs to a different week than 2000-01-01 and 2000-01-02
INSERT INTO partitioned_by_week VALUES ('2000-01-01', 1), ('2000-01-02', 2), ('2000-01-03', 3);
INSERT INTO partitioned_by_week VALUES ('2000-01-03', 4), ('2000-01-03', 5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_week' AND active ORDER BY name;
OPTIMIZE TABLE partitioned_by_week PARTITION '2000-01-03' FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_week' AND active ORDER BY name;

SELECT 'Sum before DROP PARTITION:';
SELECT sum(x) FROM partitioned_by_week;
ALTER TABLE partitioned_by_week DROP PARTITION '1999-12-27';
SELECT 'Sum after DROP PARTITION:';
SELECT sum(x) FROM partitioned_by_week;

DROP TABLE partitioned_by_week;

SELECT '*** Partitioned by a (Date, UInt8) tuple ***';

DROP TABLE IF EXISTS partitioned_by_tuple;
CREATE TABLE partitioned_by_tuple(d Date, x UInt8, y UInt8) ENGINE MergeTree ORDER BY x PARTITION BY (d, x);

INSERT INTO partitioned_by_tuple VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2), ('2000-01-02', 1, 3);
INSERT INTO partitioned_by_tuple VALUES ('2000-01-02', 1, 4), ('2000-01-01', 1, 5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_tuple' AND active ORDER BY name;
OPTIMIZE TABLE partitioned_by_tuple PARTITION ('2000-01-01', 1) FINAL;
OPTIMIZE TABLE partitioned_by_tuple PARTITION ('2000-01-02', 1) FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_tuple' AND active ORDER BY name;

SELECT 'Sum before DETACH PARTITION:';
SELECT sum(y) FROM partitioned_by_tuple;
ALTER TABLE partitioned_by_tuple DETACH PARTITION ID '20000101-1';
SELECT 'Sum after DETACH PARTITION:';
SELECT sum(y) FROM partitioned_by_tuple;

DROP TABLE partitioned_by_tuple;

SELECT '*** Partitioned by String ***';

DROP TABLE IF EXISTS partitioned_by_string;
CREATE TABLE partitioned_by_string(s String, x UInt8) ENGINE = MergeTree PARTITION BY s ORDER BY x;

INSERT INTO partitioned_by_string VALUES ('aaa', 1), ('aaa', 2), ('bbb', 3);
INSERT INTO partitioned_by_string VALUES ('bbb', 4), ('aaa', 5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_string' AND active ORDER BY name;
OPTIMIZE TABLE partitioned_by_string PARTITION 'aaa' FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_string' AND active ORDER BY name;

SELECT 'Sum before DROP PARTITION:';
SELECT sum(x) FROM partitioned_by_string;
ALTER TABLE partitioned_by_string DROP PARTITION 'bbb';
SELECT 'Sum after DROP PARTITION:';
SELECT sum(x) FROM partitioned_by_string;

DROP TABLE partitioned_by_string;

SELECT '*** Table without columns with fixed size ***';

DROP TABLE IF EXISTS without_fixed_size_columns;
CREATE TABLE without_fixed_size_columns(s String) ENGINE MergeTree PARTITION BY length(s) ORDER BY s;

INSERT INTO without_fixed_size_columns VALUES ('a'), ('aa'), ('b'), ('cc');

SELECT 'Parts:';
SELECT partition, name, rows FROM system.parts WHERE database = currentDatabase() AND table = 'without_fixed_size_columns' AND active ORDER BY name;

SELECT 'Before DROP PARTITION:';
SELECT * FROM without_fixed_size_columns ORDER BY s;
ALTER TABLE without_fixed_size_columns DROP PARTITION 1;
SELECT 'After DROP PARTITION:';
SELECT * FROM without_fixed_size_columns ORDER BY s;

DROP TABLE without_fixed_size_columns;
