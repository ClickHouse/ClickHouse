SELECT 'simple partition key:';
DROP TABLE IF EXISTS table1;
CREATE TABLE table1 (id Int64, v UInt64)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/tables/table', '1', v)
PARTITION BY id % 200 ORDER BY id;
INSERT INTO table1 SELECT number-205, number FROM numbers(10);
INSERT INTO table1 SELECT number-205, number FROM numbers(400, 10);
SELECT toInt64(partition) as p FROM system.parts WHERE table='table1' ORDER BY p;

SELECT 'complex partition key:';
DROP TABLE IF EXISTS table2;
CREATE TABLE table2 (id Int64, v UInt64)
ENGINE = MergeTree()
PARTITION BY (toInt32(id / 2) % 3, id % 200) ORDER BY id;
INSERT INTO table2 SELECT number-205, number FROM numbers(10);
INSERT INTO table2 SELECT number-205, number FROM numbers(400, 10);
SELECT partition as p FROM system.parts WHERE table='table2' ORDER BY p;

SELECT 'comparison:';
SELECT v, v-205 as vv, moduloLegacy(vv, 200), modulo(vv, 200) FROM table1 ORDER BY v;

