# Tags: no-s3-storage

CREATE TABLE IF NOT EXISTS inmemory_insert_test ON CLUSTER test_shard_localhost (id String, report_time Int64) 
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/inmemory_insert_test', 'r1')
PARTITION BY (toYYYYMMDD(toDateTime(report_time/1000)))
ORDER BY (report_time, id)
SETTINGS min_rows_for_compact_part = 10, index_granularity = 8192;

TRUNCATE TABLE inmemory_insert_test;

INSERT INTO inmemory_insert_test(id, report_time) VALUES('abcdefghijklmnopqrstuvwxyz', '1675326231000');
INSERT INTO inmemory_insert_test(id, report_time) VALUES('a1234567890123456789012345', '1675326231000');

SELECT COUNT(1) FROM inmemory_insert_test;
DROP TABLE inmemory_insert_test;
