-- Tags: long, zookeeper, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

DROP TABLE IF EXISTS mutations_and_quorum1 SYNC;
DROP TABLE IF EXISTS mutations_and_quorum2 SYNC;

CREATE TABLE mutations_and_quorum1 (`server_date` Date, `something` String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01090/mutations_and_quorum', '1') PARTITION BY toYYYYMM(server_date) ORDER BY (server_date, something);
CREATE TABLE mutations_and_quorum2 (`server_date` Date, `something` String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01090/mutations_and_quorum', '2') PARTITION BY toYYYYMM(server_date) ORDER BY (server_date, something);

-- Should not be larger then 600e6 (default timeout in clickhouse-test)
SET insert_quorum=2, insert_quorum_parallel=0, insert_quorum_timeout=300e3;

INSERT INTO mutations_and_quorum1 VALUES ('2019-01-01', 'test1'), ('2019-02-01', 'test2'), ('2019-03-01', 'test3'), ('2019-04-01', 'test4'), ('2019-05-01', 'test1'), ('2019-06-01', 'test2'), ('2019-07-01', 'test3'), ('2019-08-01', 'test4'), ('2019-09-01', 'test1'), ('2019-10-01', 'test2'), ('2019-11-01', 'test3'), ('2019-12-01', 'test4');

ALTER TABLE mutations_and_quorum1 DELETE WHERE something = 'test1' SETTINGS mutations_sync=2;

SELECT COUNT() FROM mutations_and_quorum1;
SELECT COUNT() FROM mutations_and_quorum2;

SELECT COUNT() FROM system.mutations WHERE database = currentDatabase() AND table like 'mutations_and_quorum%' and is_done = 0;

DROP TABLE IF EXISTS mutations_and_quorum1 SYNC;
DROP TABLE IF EXISTS mutations_and_quorum2 SYNC;
