DROP TABLE IF EXISTS test.sample_merge_tree;

CREATE TABLE test.sample_merge_tree (dt DateTime, x UInt64) ENGINE = MergeTree PARTITION BY toYYYYMMDD(dt) ORDER BY x SETTINGS min_merge_bytes_to_use_direct_io=1, index_granularity = 8192;

INSERT INTO test.sample_merge_tree VALUES (toDateTime('2018-10-31 05:05:00'), 0), (toDateTime('2018-10-31 06:06:00'), 10), (toDateTime('2018-10-28 10:00:00'), 20);

OPTIMIZE TABLE test.sample_merge_tree FINAL;

SELECT * FROM test.sample_merge_tree ORDER BY x;

DROP TABLE IF EXISTS test.sample_merge_tree;
