CREATE TABLE test (
    `c_id` String,
    `p_id` String,
    `d` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/test_table', '1')
ORDER BY (c_id, p_id);

INSERT INTO test SELECT '1', '11', '111' FROM numbers(30);

INSERT INTO test SELECT '2', '22', '22' FROM numbers(30);

set mutations_sync=0;

ALTER TABLE test UPDATE d = d || toString(sleepEachRow(0.1)) where 1;

ALTER TABLE test ADD PROJECTION d_order ( SELECT min(c_id) GROUP BY `d`);
ALTER TABLE test MATERIALIZE PROJECTION d_order;
ALTER TABLE test DROP PROJECTION d_order SETTINGS mutations_sync = 2; --{serverError BAD_ARGUMENTS}

-- just to wait prev mutation
ALTER TABLE test DELETE where d = 'Hello' SETTINGS mutations_sync = 2;

ALTER TABLE test DROP PROJECTION d_order SETTINGS mutations_sync = 2;

select * from test format Null;

DROP TABLE test;
