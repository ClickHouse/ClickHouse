-- Tags: zookeeper, no-parallel-replicas

-- Test for chained RENAME COLUMN mutations applied to a part that was attached
-- with old column names. When multiple renames like c0->c1->c2->...->cN are applied
-- together to a part that has column c0, the rename chain must be properly resolved.
-- Without the fix, the mutated part would end up with empty columns, causing
-- a LOGICAL_ERROR exception in getTotalColumnsSize.

SET mutations_sync = 0;

-- Case 1: Two renames (c0 -> c3 -> c4)
DROP TABLE IF EXISTS test_chained_rename_1;

CREATE TABLE test_chained_rename_1 (c0 String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_chained_rename_1', 'r1')
ORDER BY tuple()
PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO test_chained_rename_1 VALUES ('two_renames');

ALTER TABLE test_chained_rename_1 DETACH PARTITION tuple();

ALTER TABLE test_chained_rename_1 RENAME COLUMN c0 TO c3;
ALTER TABLE test_chained_rename_1 RENAME COLUMN c3 TO c4;

ALTER TABLE test_chained_rename_1 ATTACH PARTITION tuple();
SYSTEM SYNC REPLICA test_chained_rename_1;

SELECT * FROM test_chained_rename_1;

DROP TABLE test_chained_rename_1;

-- Case 2: Three renames (c0 -> c1 -> c2 -> c3)
DROP TABLE IF EXISTS test_chained_rename_2;

CREATE TABLE test_chained_rename_2 (c0 String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_chained_rename_2', 'r1')
ORDER BY tuple()
PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO test_chained_rename_2 VALUES ('three_renames');

ALTER TABLE test_chained_rename_2 DETACH PARTITION tuple();

ALTER TABLE test_chained_rename_2 RENAME COLUMN c0 TO c1;
ALTER TABLE test_chained_rename_2 RENAME COLUMN c1 TO c2;
ALTER TABLE test_chained_rename_2 RENAME COLUMN c2 TO c3;

ALTER TABLE test_chained_rename_2 ATTACH PARTITION tuple();
SYSTEM SYNC REPLICA test_chained_rename_2;

SELECT * FROM test_chained_rename_2;

DROP TABLE test_chained_rename_2;

-- Case 3: Five renames (c0 -> c1 -> c2 -> c3 -> c4 -> c5)
DROP TABLE IF EXISTS test_chained_rename_3;

CREATE TABLE test_chained_rename_3 (c0 String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_chained_rename_3', 'r1')
ORDER BY tuple()
PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO test_chained_rename_3 VALUES ('five_renames');

ALTER TABLE test_chained_rename_3 DETACH PARTITION tuple();

ALTER TABLE test_chained_rename_3 RENAME COLUMN c0 TO c1;
ALTER TABLE test_chained_rename_3 RENAME COLUMN c1 TO c2;
ALTER TABLE test_chained_rename_3 RENAME COLUMN c2 TO c3;
ALTER TABLE test_chained_rename_3 RENAME COLUMN c3 TO c4;
ALTER TABLE test_chained_rename_3 RENAME COLUMN c4 TO c5;

ALTER TABLE test_chained_rename_3 ATTACH PARTITION tuple();
SYSTEM SYNC REPLICA test_chained_rename_3;

SELECT * FROM test_chained_rename_3;

DROP TABLE test_chained_rename_3;

-- Case 4: Multiple columns with independent rename chains
DROP TABLE IF EXISTS test_chained_rename_4;

CREATE TABLE test_chained_rename_4 (a String, b String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_chained_rename_4', 'r1')
ORDER BY tuple()
PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO test_chained_rename_4 VALUES ('val_a', 'val_b');

ALTER TABLE test_chained_rename_4 DETACH PARTITION tuple();

ALTER TABLE test_chained_rename_4 RENAME COLUMN a TO a1;
ALTER TABLE test_chained_rename_4 RENAME COLUMN b TO b1;
ALTER TABLE test_chained_rename_4 RENAME COLUMN a1 TO a2;
ALTER TABLE test_chained_rename_4 RENAME COLUMN b1 TO b2;
ALTER TABLE test_chained_rename_4 RENAME COLUMN a2 TO a3;

ALTER TABLE test_chained_rename_4 ATTACH PARTITION tuple();
SYSTEM SYNC REPLICA test_chained_rename_4;

SELECT * FROM test_chained_rename_4;

DROP TABLE test_chained_rename_4;
