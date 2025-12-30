-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree - in SMT this works differently

-- Test for MergeTreeData::checkDropCommandDoesntAffectInProgressMutations() basically

DROP TABLE IF EXISTS test SYNC;
CREATE TABLE test
(
    c_id String,
    p_id String,
    d UInt32,
)
Engine = ReplicatedMergeTree('/clickhouse/tables/{database}/test/test_table', '1')
ORDER BY (c_id, p_id);

INSERT INTO test SELECT '1', '11', '111' FROM numbers(5);
ALTER TABLE test UPDATE d = d + throwIf(1) where 1 SETTINGS mutations_sync=0;
ALTER TABLE test ADD COLUMN x UInt32 default 0 SETTINGS mutations_sync=0;
ALTER TABLE test UPDATE d = x + 1 where 1 SETTINGS mutations_sync=0;

ALTER TABLE test DROP COLUMN x SETTINGS mutations_sync=2; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND command LIKE '%throwIf%' SYNC FORMAT Null;

ALTER TABLE test UPDATE x = x + 1 where 1 SETTINGS mutations_sync=2;
ALTER TABLE test DROP COLUMN x SETTINGS mutations_sync=2;
SELECT * from test format Null;
DROP TABLE test;
