-- Tags: zookeeper

-- Check that settings are correctly passed through Distributed table
DROP TABLE IF EXISTS simple;
CREATE TABLE simple (d Int8) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_00563/tables/simple', '1') ORDER BY d;

SELECT 'prefer_localhost_replica=1';
INSERT INTO TABLE FUNCTION remote('127.0.0.1', currentDatabase(), 'simple') SETTINGS prefer_localhost_replica=1, insert_deduplicate=1 VALUES (1);
INSERT INTO TABLE FUNCTION remote('127.0.0.1', currentDatabase(), 'simple') SETTINGS prefer_localhost_replica=1, insert_deduplicate=1 VALUES (1);
INSERT INTO TABLE FUNCTION remote('127.0.0.1', currentDatabase(), 'simple') SETTINGS prefer_localhost_replica=1, insert_deduplicate=0 VALUES (2);
INSERT INTO TABLE FUNCTION remote('127.0.0.1', currentDatabase(), 'simple') SETTINGS prefer_localhost_replica=1, insert_deduplicate=0 VALUES (2);
SELECT * FROM remote('127.0.0.1', currentDatabase(), 'simple') ORDER BY d;

SELECT 'prefer_localhost_replica=0';
TRUNCATE TABLE simple;
INSERT INTO TABLE FUNCTION remote('127.0.0.1', currentDatabase(), 'simple') SETTINGS prefer_localhost_replica=0, insert_deduplicate=1 VALUES (1);
INSERT INTO TABLE FUNCTION remote('127.0.0.1', currentDatabase(), 'simple') SETTINGS prefer_localhost_replica=0, insert_deduplicate=1 VALUES (1);
INSERT INTO TABLE FUNCTION remote('127.0.0.1', currentDatabase(), 'simple') SETTINGS prefer_localhost_replica=0, insert_deduplicate=0 VALUES (2);
INSERT INTO TABLE FUNCTION remote('127.0.0.1', currentDatabase(), 'simple') SETTINGS prefer_localhost_replica=0, insert_deduplicate=0 VALUES (2);
SELECT * FROM remote('127.0.0.2', currentDatabase(), 'simple') ORDER BY d;
DROP TABLE simple;
