-- Tags: long, zookeeper, no-shared-merge-tree, no-msan, no-asan, no-tsan, no-ubsan
-- no-shared-merge-tree: no replication queue

DROP TABLE IF EXISTS i20203_1 SYNC;
DROP TABLE IF EXISTS i20203_2 SYNC;

CREATE TABLE i20203_1 (a Int8)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/01715_background_checker_i20203', 'r1')
ORDER BY tuple();

CREATE TABLE i20203_2 (a Int8)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/01715_background_checker_i20203', 'r2')
ORDER BY tuple();

DETACH TABLE i20203_2;
INSERT INTO i20203_1 VALUES (2);

DETACH TABLE i20203_1;
ATTACH TABLE i20203_2;

-- sleep 10 seconds
SET function_sleep_max_microseconds_per_block = 10000000;
SELECT number from numbers(10) where sleepEachRow(1) Format Null;

SELECT num_tries < 200
FROM system.replication_queue
WHERE table = 'i20203_2' AND database = currentDatabase();

ATTACH TABLE i20203_1;

DROP TABLE i20203_1 SYNC;
DROP TABLE i20203_2 SYNC;
