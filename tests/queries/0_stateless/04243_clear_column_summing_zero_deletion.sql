-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101953
--
-- ALTER TABLE ... CLEAR COLUMN IN PARTITION on a SummingMergeTree table must
-- still trigger zero-row deletion during OPTIMIZE TABLE FINAL, matching the
-- behaviour of ALTER TABLE ... UPDATE col = 0.
--
-- The regression was introduced by https://github.com/ClickHouse/ClickHouse/pull/88860
-- ("Treat absent columns as expired"), which extended the merge-time
-- "absent-column-as-expired" optimization from TTL-finished columns to any
-- column missing from all source parts. CLEAR COLUMN drops the cleared column
-- from columns.txt entirely, so under the new optimization the column never
-- reaches SummingSortedAlgorithm and the zero-row deletion is silently bypassed.
--
-- The fix narrows the optimization to merge modes where non-key columns do not
-- affect row inclusion (Ordinary, Collapsing, Replacing, VersionedCollapsing),
-- and skips it for Summing, Aggregating, Coalescing, and Graphite modes where
-- every non-key column participates in the merge algorithm.

DROP TABLE IF EXISTS test_clear_summing;

CREATE TABLE test_clear_summing
(
    v UInt64,
    p UInt64,
    c UInt64
)
ENGINE = SummingMergeTree
PARTITION BY p
ORDER BY v;

INSERT INTO test_clear_summing VALUES (1, 1, 100);
INSERT INTO test_clear_summing VALUES (2, 2, 200);

OPTIMIZE TABLE test_clear_summing FINAL;

SELECT 'before clear';
SELECT * FROM test_clear_summing ORDER BY v;

ALTER TABLE test_clear_summing CLEAR COLUMN c IN PARTITION 1 SETTINGS mutations_sync = 2;

OPTIMIZE TABLE test_clear_summing FINAL;

SELECT 'after clear + optimize final';
SELECT * FROM test_clear_summing ORDER BY v;

-- Cross-check: ALTER UPDATE c = 0 must produce the same result.
DROP TABLE test_clear_summing;

CREATE TABLE test_clear_summing
(
    v UInt64,
    p UInt64,
    c UInt64
)
ENGINE = SummingMergeTree
PARTITION BY p
ORDER BY v;

INSERT INTO test_clear_summing VALUES (1, 1, 100);
INSERT INTO test_clear_summing VALUES (2, 2, 200);

OPTIMIZE TABLE test_clear_summing FINAL;

ALTER TABLE test_clear_summing UPDATE c = 0 WHERE v = 1 SETTINGS mutations_sync = 2;

OPTIMIZE TABLE test_clear_summing FINAL;

SELECT 'cross-check update c=0';
SELECT * FROM test_clear_summing ORDER BY v;

-- Multi-row partition: every row in partition 1 has c = 0 after CLEAR, so the
-- whole partition must collapse to nothing.
DROP TABLE test_clear_summing;

CREATE TABLE test_clear_summing
(
    v UInt64,
    p UInt64,
    c UInt64
)
ENGINE = SummingMergeTree
PARTITION BY p
ORDER BY v;

INSERT INTO test_clear_summing VALUES (1, 1, 100);
INSERT INTO test_clear_summing VALUES (2, 1, 50);
INSERT INTO test_clear_summing VALUES (3, 1, 30);

OPTIMIZE TABLE test_clear_summing FINAL;

ALTER TABLE test_clear_summing CLEAR COLUMN c IN PARTITION 1 SETTINGS mutations_sync = 2;

OPTIMIZE TABLE test_clear_summing FINAL;

SELECT 'multi-row partition after clear';
SELECT * FROM test_clear_summing ORDER BY v;

DROP TABLE test_clear_summing;

-- ALTER ADD COLUMN that never gets data written to existing parts must also
-- behave correctly during OPTIMIZE FINAL: the synthesized default-value column
-- still participates in zero-row deletion.
CREATE TABLE test_clear_summing
(
    v UInt64,
    p UInt64
)
ENGINE = SummingMergeTree
PARTITION BY p
ORDER BY v;

INSERT INTO test_clear_summing VALUES (1, 1);
INSERT INTO test_clear_summing VALUES (2, 2);

OPTIMIZE TABLE test_clear_summing FINAL;

ALTER TABLE test_clear_summing ADD COLUMN c UInt64;

OPTIMIZE TABLE test_clear_summing FINAL;

SELECT 'add column with no rewrite';
SELECT * FROM test_clear_summing ORDER BY v;

DROP TABLE test_clear_summing;

-- CoalescingMergeTree must NOT drop rows with all-default values (issue #81303
-- behaviour preserved). The fix to MergeTask.cpp keeps the cleared column in
-- the merge stream for Coalescing too, but the algorithm's last_value
-- semantics keep the row.
CREATE TABLE test_clear_coalescing
(
    v UInt64,
    p UInt64,
    c UInt64
)
ENGINE = CoalescingMergeTree
PARTITION BY p
ORDER BY v;

INSERT INTO test_clear_coalescing VALUES (1, 1, 100);
INSERT INTO test_clear_coalescing VALUES (2, 2, 200);

OPTIMIZE TABLE test_clear_coalescing FINAL;

ALTER TABLE test_clear_coalescing CLEAR COLUMN c IN PARTITION 1 SETTINGS mutations_sync = 2;

OPTIMIZE TABLE test_clear_coalescing FINAL;

SELECT 'coalescing keeps rows';
SELECT * FROM test_clear_coalescing ORDER BY v;

DROP TABLE test_clear_coalescing;

-- Ordinary MergeTree must keep the absent-column optimization (no change in
-- behaviour). The cleared column gets default values, and the row stays.
CREATE TABLE test_clear_ordinary
(
    v UInt64,
    p UInt64,
    c UInt64
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY v;

INSERT INTO test_clear_ordinary VALUES (1, 1, 100);

ALTER TABLE test_clear_ordinary CLEAR COLUMN c IN PARTITION 1 SETTINGS mutations_sync = 2;

OPTIMIZE TABLE test_clear_ordinary FINAL;

SELECT 'ordinary keeps row';
SELECT * FROM test_clear_ordinary ORDER BY v;

DROP TABLE test_clear_ordinary;
