SELECT 'simple partition key:';
DROP TABLE IF EXISTS table1 SYNC;
CREATE TABLE table1 (id Int64, v UInt64)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/test_table12', '1', v)
PARTITION BY id % 200 ORDER BY id;
INSERT INTO table1 SELECT number-205, number FROM numbers(10);
INSERT INTO table1 SELECT number-205, number FROM numbers(400, 10);
SELECT toInt64(partition) as p FROM system.parts WHERE table='table1' and database=currentDatabase() ORDER BY p;

select 'where id % 200 = +-2:';
select id from table1 where id % 200 = 2 OR id % 200 = -2 order by id;
select 'where id % 200 > 0:';
select id from table1 where id % 200 > 0 order by id;
select 'where id % 200 < 0:';
select id from table1 where id % 200 < 0 order by id;

SELECT 'tuple as partition key:';
DROP TABLE IF EXISTS table2;
CREATE TABLE table2 (id Int64, v UInt64)
ENGINE = MergeTree()
PARTITION BY (toInt32(id / 2) % 3, id % 200) ORDER BY id;
INSERT INTO table2 SELECT number-205, number FROM numbers(10);
INSERT INTO table2 SELECT number-205, number FROM numbers(400, 10);
SELECT partition as p FROM system.parts WHERE table='table2' and database=currentDatabase() ORDER BY p;

SELECT 'recursive modulo partition key:';
DROP TABLE IF EXISTS table3;
CREATE TABLE table3 (id Int64, v UInt64)
ENGINE = MergeTree()
PARTITION BY (id % 200, (id % 200) % 10, toInt32(round((id % 200) / 2, 0))) ORDER BY id;
INSERT INTO table3 SELECT number-205, number FROM numbers(10);
INSERT INTO table3 SELECT number-205, number FROM numbers(400, 10);
SELECT partition as p FROM system.parts WHERE table='table3' and database=currentDatabase() ORDER BY p;

DETACH TABLE table3;
ATTACH TABLE table3;
SELECT 'After detach:';
SELECT partition as p FROM system.parts WHERE table='table3' and database=currentDatabase() ORDER BY p;

SELECT 'Indexes:';
DROP TABLE IF EXISTS table4;
CREATE TABLE table4 (id Int64, v UInt64, s String,
INDEX a (id * 2, s) TYPE minmax GRANULARITY 3
) ENGINE = MergeTree() PARTITION BY id % 10 ORDER BY v;
INSERT INTO table4 SELECT number, number, toString(number) FROM numbers(1000);
SELECT count() FROM table4 WHERE id % 10 = 7;

SELECT 'comparison:';
SELECT v, v-205 as vv, modulo(vv, 200), moduloLegacy(vv, 200) FROM table1 ORDER BY v;

DROP TABLE table1 SYNC;
DROP TABLE table2 SYNC;
DROP TABLE table3 SYNC;
DROP TABLE table4 SYNC;
