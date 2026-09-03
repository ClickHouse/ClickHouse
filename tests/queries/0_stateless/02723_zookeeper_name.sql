-- Tags: zookeeper, replica

SELECT 'Create Tables';
CREATE TABLE t1(k UInt32, v UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02723/zookeeper_name/t1', '1') ORDER BY k;

CREATE TABLE t2(k UInt32, v UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02723/zookeeper_name/t2', '1') ORDER BY k;

SELECT 'Insert Data';

INSERT INTO t1 SELECT * FROM generateRandom('k UInt32, v UInt32') LIMIT 1;
INSERT INTO t2 SELECT * FROM generateRandom('k UInt32, v UInt32') LIMIT 1;

SELECT
    table, zookeeper_name, count()
FROM system.replicas
INNER JOIN system.parts USING (database, table)
WHERE database = currentDatabase()
GROUP BY table, zookeeper_name
ORDER BY table, zookeeper_name
FORMAT CSV;

DROP TABLE t1;
DROP TABLE t2;
