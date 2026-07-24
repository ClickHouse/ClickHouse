-- A sort-key column that has an explicit `DEFAULT` expression and is missing on disk
-- (added by `ALTER ADD COLUMN` + `MODIFY ORDER BY`, then given a default by `MODIFY COLUMN`)
-- is left as a null placeholder by `fillMissingColumns`. Reads must evaluate the default
-- before `MergeOnKey` key comparisons in `readPatches`, otherwise the read crashes.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_key_column_default SYNC;

CREATE TABLE t_lwu_key_column_default (a UInt64, v String)
ENGINE = MergeTree
PRIMARY KEY a
ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_key_column_default SELECT intDiv(number, 100), 'foo' FROM numbers(10000);

ALTER TABLE t_lwu_key_column_default ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
ALTER TABLE t_lwu_key_column_default MODIFY COLUMN b UInt64 DEFAULT a + 100;

UPDATE t_lwu_key_column_default SET v = 'bar' WHERE a >= 50;

SELECT countIf(v = 'bar'), countIf(v = 'foo') FROM t_lwu_key_column_default SETTINGS max_block_size = 256;
SELECT count() FROM t_lwu_key_column_default WHERE v = 'bar' SETTINGS max_block_size = 256;
SELECT count() FROM t_lwu_key_column_default PREWHERE v = 'foo';
SELECT sum(b = a + 100) FROM t_lwu_key_column_default;

DROP TABLE t_lwu_key_column_default SYNC;
