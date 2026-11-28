-- A column the part stores is dropped by a mutation that is still pending and added again with a `DEFAULT`.
-- The reader ignores the data of the dropped column, so the new column follows a lightweight `UPDATE` of the
-- column its `DEFAULT` reads, as does a `DEFAULT` column that reads it.

SET alter_sync = 0, mutations_sync = 0;

DROP TABLE IF EXISTS t_lwu_pending_drop_readd;

CREATE TABLE t_lwu_pending_drop_readd (id UInt32, z UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_pending_drop_readd VALUES (0, 999);
SYSTEM STOP MERGES t_lwu_pending_drop_readd;

ALTER TABLE t_lwu_pending_drop_readd DROP COLUMN z;
ALTER TABLE t_lwu_pending_drop_readd ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_pending_drop_readd ADD COLUMN z UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_pending_drop_readd ADD COLUMN y UInt32 DEFAULT z + 1;

UPDATE t_lwu_pending_drop_readd SET a = 5 WHERE 1;

SELECT id, a, z, y FROM t_lwu_pending_drop_readd;
SELECT z, y FROM t_lwu_pending_drop_readd;
SELECT y FROM t_lwu_pending_drop_readd;

DROP TABLE t_lwu_pending_drop_readd;
