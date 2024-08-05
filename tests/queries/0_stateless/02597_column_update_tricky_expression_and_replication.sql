CREATE TABLE test (
    `c_id` String,
    `p_id` String,
    `d` UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/test_table', '1')
ORDER BY (c_id, p_id);

INSERT INTO test SELECT '1', '11', '111' FROM numbers(5);

SET mutations_sync=0;
SYSTEM ENABLE FAILPOINT infinite_sleep;

ALTER TABLE test UPDATE d = d + sleepEachRow(0.3) where 1;

ALTER TABLE test ADD COLUMN x UInt32 default 0;
ALTER TABLE test UPDATE d = x + 1 where 1;

SYSTEM DISABLE FAILPOINT infinite_sleep;
ALTER TABLE test DROP COLUMN x SETTINGS mutations_sync = 2; --{serverError BAD_ARGUMENTS}

ALTER TABLE test UPDATE x = x + 1 where 1 SETTINGS mutations_sync = 2;
ALTER TABLE test DROP COLUMN x SETTINGS mutations_sync = 2;

SELECT * from test format Null;

DROP TABLE test;

