-- Tags: long, zookeeper

DROP TABLE IF EXISTS i20203_1;
DROP TABLE IF EXISTS i20203_2;

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
SELECT number from numbers(10) where sleepEachRow(1) Format Null;

SELECT num_tries < 50
FROM system.replication_queue
WHERE table = 'i20203_2' AND database = currentDatabase();

ATTACH TABLE i20203_1;

DROP TABLE IF EXISTS i20203_1;
DROP TABLE IF EXISTS i20203_2;
