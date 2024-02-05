CREATE TABLE test (
    `c_id` String,
    `p_id` String,
    `d` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/test_table', '1')
ORDER BY (c_id, p_id);

INSERT INTO test SELECT '1', '11', '111' FROM numbers(3);

INSERT INTO test SELECT '2', '22', '22' FROM numbers(3);

set mutations_sync=0;

ALTER TABLE test UPDATE d = d || toString(sleepEachRow(0.3)) where 1;

ALTER TABLE test ADD COLUMN x UInt32 default 0;
ALTER TABLE test UPDATE d = d || '1' where x = 42;
ALTER TABLE test DROP COLUMN x SETTINGS mutations_sync = 2; --{serverError 36}

ALTER TABLE test UPDATE x = x + 1 where 1 SETTINGS mutations_sync = 2;

ALTER TABLE test DROP COLUMN x SETTINGS mutations_sync = 2;

select * from test format Null;

DROP TABLE test;
