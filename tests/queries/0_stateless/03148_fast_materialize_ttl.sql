SET alter_sync = 2;

SELECT 'Test MergeTree to modify TTL.';
DROP TABLE IF EXISTS test_fast_ttl;
CREATE TABLE test_fast_ttl (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = MergeTree()
TTL create_time + toIntervalDay(300)
ORDER BY id;

INSERT INTO test_fast_ttl SELECT number, 'AAA', date_sub(day, 100, now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'BBB', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'CCC', now() from numbers(2000);
SELECT COUNT(*) FROM test_fast_ttl;

SELECT 'Test shortening TTL.';
ALTER TABLE test_fast_ttl MODIFY TTL create_time + INTERVAL 10 DAY;
SELECT COUNT(*) FROM test_fast_ttl;

SELECT 'Perform Merge after inserting data.';
INSERT INTO test_fast_ttl SELECT number, 'DDD', if(number >= 1000, date_sub(day, 20, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl FINAL;
SELECT COUNT(*) FROM test_fast_ttl;

SELECT 'Testing for extended TTL.';
ALTER TABLE test_fast_ttl MODIFY TTL create_time + INTERVAL 30 DAY;
SELECT COUNT(*) FROM test_fast_ttl;

SELECT 'Perform Merge after inserting data.';
INSERT INTO test_fast_ttl SELECT number, 'EEE', if(number >= 1000, date_sub(day, 20, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl FINAL;
SELECT COUNT(*) FROM test_fast_ttl;

DROP TABLE IF EXISTS test_fast_ttl;


SELECT 'Test ReplicatedMergeTree to modify TTL.';
DROP TABLE IF EXISTS test_fast_ttl_replica1;
CREATE TABLE test_fast_ttl_replica1 (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_fast_ttl_replica', '1')
TTL create_time + toIntervalDay(300)
ORDER BY id;

DROP TABLE IF EXISTS test_fast_ttl_replica2;
CREATE TABLE test_fast_ttl_replica2 (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_fast_ttl_replica', '2')
TTL create_time + toIntervalDay(300)
ORDER BY id;

INSERT INTO test_fast_ttl_replica1 SELECT number, 'AAA', date_sub(day, 100, now()) from numbers(2000);
INSERT INTO test_fast_ttl_replica1 SELECT number, 'BBB', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl_replica1 SELECT number, 'CCC', now() from numbers(2000);
SYSTEM SYNC REPLICA test_fast_ttl_replica2;
SELECT COUNT(*) FROM test_fast_ttl_replica2;

SELECT 'Test shortening TTL.';
ALTER TABLE test_fast_ttl_replica1 MODIFY TTL create_time + INTERVAL 10 DAY;
SELECT COUNT(*) FROM test_fast_ttl_replica2;

SELECT 'Perform Merge after inserting data.';
INSERT INTO test_fast_ttl_replica1 SELECT number, 'DDD', if(number >= 1000, date_sub(day, 20, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl_replica1 FINAL;
SELECT COUNT(*) FROM test_fast_ttl_replica2;

SELECT 'Testing for extended TTL.';
ALTER TABLE test_fast_ttl_replica1 MODIFY TTL create_time + INTERVAL 30 DAY;
SELECT COUNT(*) FROM test_fast_ttl_replica2;

SELECT 'Perform Merge after inserting data.';
INSERT INTO test_fast_ttl_replica1 SELECT number, 'EEE', if(number >= 1000, date_sub(day, 20, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl_replica1 FINAL;
SYSTEM SYNC REPLICA test_fast_ttl_replica2;
SELECT COUNT(*) FROM test_fast_ttl_replica2;

DROP TABLE IF EXISTS test_fast_ttl_replica1;
DROP TABLE IF EXISTS test_fast_ttl_replica2;
