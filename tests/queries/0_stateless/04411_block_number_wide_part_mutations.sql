DROP TABLE IF EXISTS t;

-- Wide + full storage (so columns are hardlinked) with persisted _block_number/_block_offset.
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         always_use_copy_instead_of_hardlinks = 0;

SET mutations_sync = 1;

INSERT INTO t VALUES (1, 1), (2, 1);
INSERT INTO t VALUES (3, 1), (4, 1);

SELECT 'insert', a, b, _block_number, _block_offset FROM t ORDER BY a;
SELECT '';

-- Re-home before any read: rows get fresh block numbers, offsets become part offsets.
ALTER TABLE t DETACH PARTITION tuple();
ALTER TABLE t ATTACH PARTITION tuple();
SELECT 'DETACH ATTACH';
SELECT 'attach', a, b, _block_number, _block_offset FROM t ORDER BY a;
SELECT '';

-- Partial mutations must keep the post-attach block, not the stale on-disk one.
ALTER TABLE t UPDATE b = 2 WHERE a = 1;
SELECT 'ALTER TABLE t UPDATE b = 2 WHERE a = 1';
SELECT 'update', a, b, _block_number, _block_offset FROM t ORDER BY a;
SELECT '';

ALTER TABLE t UPDATE b = 3 WHERE a = 2;
SELECT 'ALTER TABLE t UPDATE b = 3 WHERE a = 2';
SELECT 'update_again', a, b, _block_number, _block_offset FROM t ORDER BY a;
SELECT '';

ALTER TABLE t DELETE WHERE a = 4;
SELECT 'ALTER TABLE t DELETE WHERE a = 4';
SELECT 'delete', a, b, _block_number, _block_offset FROM t ORDER BY a;
SELECT '';

OPTIMIZE TABLE t FINAL;
SELECT 'OPTIMIZE TABLE t FINAL';
SELECT 'merge', a, b, _block_number, _block_offset FROM t ORDER BY a;
SELECT '';

INSERT INTO t VALUES (5, 1), (6, 1);
OPTIMIZE TABLE t FINAL;
SELECT 'INSERT INTO t VALUES (5, 1), (6, 1)';
SELECT 'merge_after_insert', a, b, _block_number, _block_offset FROM t ORDER BY a;

DROP TABLE t;
