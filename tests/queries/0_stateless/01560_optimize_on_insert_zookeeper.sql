-- Tags: zookeeper, no-parallel

DROP TABLE IF EXISTS empty1;
DROP TABLE IF EXISTS empty2;

SELECT 'Check creating empty parts';

CREATE TABLE empty1 (key UInt32, val UInt32, date Datetime)
ENGINE=ReplicatedSummingMergeTree('/clickhouse/01560_optimize_on_insert', '1', val)
PARTITION BY date ORDER BY key;

CREATE TABLE empty2 (key UInt32, val UInt32, date Datetime)
ENGINE=ReplicatedSummingMergeTree('/clickhouse/01560_optimize_on_insert', '2', val)
PARTITION BY date ORDER BY key;

INSERT INTO empty2 VALUES (1, 1, '2020-01-01'), (1, 1, '2020-01-01'), (1, -2, '2020-01-01');

SYSTEM SYNC REPLICA empty1;

SELECT * FROM empty1 ORDER BY key;
SELECT * FROM empty2 ORDER BY key;

SELECT table, partition, active FROM system.parts where table = 'empty1' and database=currentDatabase() and active = 1;
SELECT table, partition, active FROM system.parts where table = 'empty2' and database=currentDatabase() and active = 1;

DETACH table empty1;
DETACH table empty2;
ATTACH table empty1;
ATTACH table empty2;

SELECT * FROM empty1 ORDER BY key;
SELECT * FROM empty2 ORDER BY key;

SELECT table, partition, active FROM system.parts where table = 'empty1' and database=currentDatabase() and active = 1;
SELECT table, partition, active FROM system.parts where table = 'empty2' and database=currentDatabase() and active = 1;

DROP TABLE IF EXISTS empty1;
DROP TABLE IF EXISTS empty2;
