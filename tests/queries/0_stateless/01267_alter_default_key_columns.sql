DROP TABLE IF EXISTS test_alter;
CREATE TABLE test_alter (x Date, s String) ENGINE = MergeTree ORDER BY s PARTITION BY x;
ALTER TABLE test_alter MODIFY COLUMN s DEFAULT 'Hello';
ALTER TABLE test_alter MODIFY COLUMN x DEFAULT '2000-01-01';
DESCRIBE TABLE test_alter;
DROP TABLE test_alter;

DROP TABLE IF EXISTS test_alter_r1;
DROP TABLE IF EXISTS test_alter_r2;

CREATE TABLE test_alter_r1 (x Date, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter', 'r1') ORDER BY s PARTITION BY x;
CREATE TABLE test_alter_r2 (x Date, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter', 'r2') ORDER BY s PARTITION BY x;

ALTER TABLE test_alter_r1 MODIFY COLUMN s DEFAULT 'Hello';
ALTER TABLE test_alter_r2 MODIFY COLUMN x DEFAULT '2000-01-01';

SYSTEM SYNC REPLICA test_alter_r1;
SYSTEM SYNC REPLICA test_alter_r2;

DESCRIBE TABLE test_alter_r1;
DESCRIBE TABLE test_alter_r2;

SYSTEM RESTART REPLICAS;
DESCRIBE TABLE test_alter_r1;
DESCRIBE TABLE test_alter_r2;

DROP TABLE test_alter_r1;
DROP TABLE test_alter_r2;
