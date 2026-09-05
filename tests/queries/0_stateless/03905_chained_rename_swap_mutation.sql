-- Tags: zookeeper, no-parallel-replicas, no-shared-merge-tree
-- no-shared-merge-tree: SharedMergeTree applies no alter conversions to a re-attached part, so it
-- returns the columns un-renamed and the expected output below cannot be written down for it. The
-- same defect excludes plain MergeTree. Verified: on SharedMergeTree this test yields 2 1 3 where
-- ReplicatedMergeTree yields 1 2 3, identically before and after this fix.

-- A chain of RENAME COLUMN mutations that composes into a swap (a -> a1 -> b, b -> b1 -> a)
-- can be applied to a part in one go, e.g. when the part was detached before the renames and
-- re-attached after them, so it still has the original column names. The composed rename map
-- then contains a cycle (a -> b, b -> a). The next mutation that rewrites only some columns
-- and hardlinks the rest used to apply this map to the inherited checksums one entry at a
-- time in place, so the first rename overwrote the entry the second one still had to read.
-- The mutation then committed a part whose columns.txt lists a column with no streams in
-- checksums.txt: reading it failed with a logical error "Stream ... is not found" and the
-- part was detached as broken on the next server start.

DROP TABLE IF EXISTS test_rename_swap;

CREATE TABLE test_rename_swap (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_rename_swap', 'r1')
ORDER BY tuple()
PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO test_rename_swap VALUES (1, 2, 3);

-- Detach the partition so the part keeps the original column names and metadata version.
ALTER TABLE test_rename_swap DETACH PARTITION tuple();

-- Rename a -> a1 -> b and b -> b1 -> a while the table has no parts.
-- Both mutations complete trivially (no parts to mutate).
ALTER TABLE test_rename_swap RENAME COLUMN a TO a1, RENAME COLUMN b TO b1;
ALTER TABLE test_rename_swap RENAME COLUMN a1 TO b, RENAME COLUMN b1 TO a;

-- Attach the partition back. The part has columns a, b, c while the table has b, a, c;
-- the composed rename chain is a swap of a and b.
ALTER TABLE test_rename_swap ATTACH PARTITION tuple();

-- The swap is applied on the fly while reading.
SELECT * FROM test_rename_swap ORDER BY ALL;

-- A mutation that rewrites only column c and hardlinks the rest applies the composed swap
-- to the files and checksums of the new part.
ALTER TABLE test_rename_swap UPDATE c = c + 10 WHERE 1 SETTINGS mutations_sync = 2;

SELECT * FROM test_rename_swap ORDER BY ALL;

CHECK TABLE test_rename_swap SETTINGS check_query_single_value_result = 1;

DROP TABLE test_rename_swap;

-- Same composed swap, but the mutation also rewrites one of the swapped columns.
-- The writer produces the stream a rename targets: the rename must yield to the
-- writer (previously the mutation either got stuck failing to open the hardlinked
-- read-only file, or committed inconsistent checksums).

DROP TABLE IF EXISTS test_rename_swap_upd;

CREATE TABLE test_rename_swap_upd (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_rename_swap_upd', 'r1')
ORDER BY tuple()
PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO test_rename_swap_upd VALUES (1, 2, 3);

ALTER TABLE test_rename_swap_upd DETACH PARTITION tuple();
ALTER TABLE test_rename_swap_upd RENAME COLUMN a TO a1, RENAME COLUMN b TO b1;
ALTER TABLE test_rename_swap_upd RENAME COLUMN a1 TO b, RENAME COLUMN b1 TO a;
ALTER TABLE test_rename_swap_upd ATTACH PARTITION tuple();

ALTER TABLE test_rename_swap_upd UPDATE b = b + 100 WHERE 1 SETTINGS mutations_sync = 2;

SELECT * FROM test_rename_swap_upd ORDER BY ALL;

ALTER TABLE test_rename_swap_upd UPDATE a = a + 100 WHERE 1 SETTINGS mutations_sync = 2;

SELECT * FROM test_rename_swap_upd ORDER BY ALL;

CHECK TABLE test_rename_swap_upd SETTINGS check_query_single_value_result = 1;

DROP TABLE test_rename_swap_upd;
