-- Tags: no-s3-storage

DROP TABLE IF EXISTS inmemory_test;

CREATE TABLE inmemory_test (d Date, id String)
ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/inmemory_test', 'r1')
PARTITION BY toYYYYMMDD(d) ORDER BY (d, id)
SETTINGS min_rows_for_compact_part = 10, index_granularity = 8192;

INSERT INTO inmemory_test(d, id) VALUES('2023-01-01', 'abcdefghijklmnopqrstuvwxyz');
INSERT INTO inmemory_test(d, id) VALUES('2023-01-01', 'a1234567890123456789012345');

SELECT COUNT(1) FROM inmemory_test;

DROP TABLE inmemory_test;
