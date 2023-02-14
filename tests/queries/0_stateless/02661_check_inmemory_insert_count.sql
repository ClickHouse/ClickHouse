CREATE TABLE inmemory_insert_test ON CLUSTER test_shard_localhost (id String, report_time Int64) 
ENGINE = ReplicatedMergeTree('/clickhouse/test_02661/inmemory_insert_test', 'r1')
PARTITION BY (toYYYYMMDD(toDateTime(report_time/1000)))
ORDER BY (report_time, id)
SETTINGS min_rows_for_compact_part = 10;

INSERT INTO inmemory_insert_test(id, report_time) VALUES('abcdefghijklmnopqrstuvwxyz', '1675326231000');
INSERT INTO inmemory_insert_test(id, report_time) VALUES('a1234567890123456789012345', '1675326231000');

SELECT COUNT(1) FROM inmemory_insert_test;