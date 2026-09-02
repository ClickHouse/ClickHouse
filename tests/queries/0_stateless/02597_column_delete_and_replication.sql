-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree - in SMT this works differently

-- Test for MergeTreeData::checkDropCommandDoesntAffectInProgressMutations() basically

CREATE TABLE test (
    `c_id` String,
    `p_id` String,
    `d` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/test_table', '1')
ORDER BY (c_id, p_id);

set mutations_sync=0;

INSERT INTO test SELECT '1', '11', '111' FROM numbers(3);
INSERT INTO test SELECT '2', '22', '22' FROM numbers(3);

-- this mutation will run in background and will block next mutation
ALTER TABLE test UPDATE d = d || throwIf(1) where 1;

-- this mutation cannot be started until previuos ALTER finishes (in background), and will lead to DROP COLUMN failed with BAD_ARGUMENTS
ALTER TABLE test ADD COLUMN x UInt32 default 0;
ALTER TABLE test UPDATE d = d || '1' where x = 42;
ALTER TABLE test DROP COLUMN x SETTINGS mutations_sync = 2; --{serverError BAD_ARGUMENTS}

-- unblock
KILL MUTATION WHERE database = currentDatabase() AND command LIKE '%throwIf%' SYNC FORMAT Null;
ALTER TABLE test UPDATE x = x + 1 where 1 SETTINGS mutations_sync = 2;

ALTER TABLE test DROP COLUMN x SETTINGS mutations_sync = 2;

select * from test format Null;

DROP TABLE test;
