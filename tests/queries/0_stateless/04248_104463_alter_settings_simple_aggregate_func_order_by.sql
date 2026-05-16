-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104463
--
-- Settings-only and comment-only ALTER on a MergeTree table whose ORDER BY
-- references a SimpleAggregateFunction column (created with
-- `allow_suspicious_primary_key = 1`) must succeed even after that setting
-- is disabled in the session that runs the ALTER.
--
-- Before the fix, `StorageMergeTree::alter` invoked `verifySortingKey`
-- unconditionally before the early-return branches for `isSettingsAlter`
-- and `isCommentAlter`, mirroring neither `StorageReplicatedMergeTree`
-- nor the user's intent. As a result, harmless ALTERs that did not touch
-- the sorting key were rejected with `DATA_TYPE_CANNOT_BE_USED_IN_KEY`.

DROP TABLE IF EXISTS t_104463_mt;
DROP TABLE IF EXISTS t_104463_amt;

-- Plain MergeTree with a suspicious column in ORDER BY.
SET allow_suspicious_primary_key = 1;
CREATE TABLE t_104463_mt
(
    key   Int,
    value SimpleAggregateFunction(sum, UInt64)
)
ENGINE = MergeTree
ORDER BY (key, value)
SETTINGS merge_with_ttl_timeout = 30;

-- AggregatingMergeTree variant from the original report.
CREATE TABLE t_104463_amt
(
    key   Int,
    value SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (key, value)
SETTINGS merge_with_ttl_timeout = 30;

SET allow_suspicious_primary_key = 0;

-- Settings-only ALTER must not re-validate the sorting key.
ALTER TABLE t_104463_mt  MODIFY SETTING merge_with_ttl_timeout = 60;
ALTER TABLE t_104463_amt MODIFY SETTING merge_with_ttl_timeout = 60;

ALTER TABLE t_104463_mt  RESET SETTING merge_with_ttl_timeout;
ALTER TABLE t_104463_amt RESET SETTING merge_with_ttl_timeout;

-- Comment-only ALTER must not re-validate the sorting key.
ALTER TABLE t_104463_mt  MODIFY COMMENT 'table comment';
ALTER TABLE t_104463_amt MODIFY COMMENT 'table comment';

ALTER TABLE t_104463_mt  COMMENT COLUMN value 'column comment';
ALTER TABLE t_104463_amt COMMENT COLUMN value 'column comment';

-- ALTERs that could change the sorting key (e.g. ADD COLUMN) must still be
-- rejected because the existing key contains a suspicious type and the user
-- has not re-enabled `allow_suspicious_primary_key`.
ALTER TABLE t_104463_mt  ADD COLUMN extra Int; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
ALTER TABLE t_104463_amt ADD COLUMN extra Int; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

-- Re-enabling the setting allows the same ALTER to go through, exactly as
-- it does for ReplicatedMergeTree.
SET allow_suspicious_primary_key = 1;
ALTER TABLE t_104463_mt  ADD COLUMN extra Int;
ALTER TABLE t_104463_amt ADD COLUMN extra Int;

SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_104463_mt'  AND name = 'extra';
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_104463_amt' AND name = 'extra';

DROP TABLE t_104463_mt;
DROP TABLE t_104463_amt;
