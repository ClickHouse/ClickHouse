-- A sorting key column that the part does not store, whose DEFAULT reads a non-key column.
-- The patch part is sorted by the real key, so unless the DEFAULT's input is read alongside it the
-- key is computed from that input's type default and the patch matches nothing.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

-- The first read step is the main step.
DROP TABLE IF EXISTS t_lwu_key_default_select SYNC;

CREATE TABLE t_lwu_key_default_select (a UInt64, v String, w String)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_key_default_select SELECT number, 'foo', 'w0' FROM numbers(10);

ALTER TABLE t_lwu_key_default_select ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
ALTER TABLE t_lwu_key_default_select MODIFY COLUMN b UInt64 DEFAULT length(w);

UPDATE t_lwu_key_default_select SET v = 'bar' WHERE a >= 5;

SELECT 'select', countIf(v = 'bar'), countIf(v = 'foo') FROM t_lwu_key_default_select;
-- Reading the DEFAULT's input must not change the answer.
SELECT 'select_reading_input', countIf(v = 'bar'), countIf(v = 'foo'), any(w) FROM t_lwu_key_default_select;
-- The first read step is a PREWHERE step.
SELECT 'prewhere', count(), any(w) FROM t_lwu_key_default_select PREWHERE v = 'bar';

DROP TABLE t_lwu_key_default_select SYNC;

-- The first read step is a lightweight delete step, and the key column is read back directly.
DROP TABLE IF EXISTS t_lwu_key_default_delete SYNC;

CREATE TABLE t_lwu_key_default_delete (a UInt64, v String, w String)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_key_default_delete SELECT number, 'foo', 'w0' FROM numbers(10);

ALTER TABLE t_lwu_key_default_delete ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
ALTER TABLE t_lwu_key_default_delete MODIFY COLUMN b UInt64 DEFAULT length(w);

DELETE FROM t_lwu_key_default_delete WHERE a >= 5 SETTINGS lightweight_delete_mode = 'lightweight_update_force';

SELECT 'delete', count() FROM t_lwu_key_default_delete;
-- b DEFAULT length(w), so b must agree with the w in its own row.
SELECT 'delete_key_column', a, b, w FROM t_lwu_key_default_delete ORDER BY a LIMIT 1;

DROP TABLE t_lwu_key_default_delete SYNC;

-- A vertical merge applies the patch part into the merged part, where losing it is permanent.
DROP TABLE IF EXISTS t_lwu_key_default_merge SYNC;

CREATE TABLE t_lwu_key_default_merge (a UInt64, v String, w String)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2',
    min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_lwu_key_default_merge;

INSERT INTO t_lwu_key_default_merge SELECT number, 'foo', 'w0' FROM numbers(5);
INSERT INTO t_lwu_key_default_merge SELECT number + 5, 'foo', 'w0' FROM numbers(5);

ALTER TABLE t_lwu_key_default_merge ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
ALTER TABLE t_lwu_key_default_merge MODIFY COLUMN b UInt64 DEFAULT length(w);

UPDATE t_lwu_key_default_merge SET v = 'bar' WHERE a >= 5;

SYSTEM START MERGES t_lwu_key_default_merge;
OPTIMIZE TABLE t_lwu_key_default_merge FINAL SETTINGS mutations_sync = 2, alter_sync = 2;

SELECT 'merge', countIf(v = 'bar'), countIf(v = 'foo') FROM t_lwu_key_default_merge;
-- Ignoring the patch parts reads only the merged part, so this is what the merge wrote.
SELECT 'merge_merged_part', countIf(v = 'bar'), countIf(v = 'foo') FROM t_lwu_key_default_merge SETTINGS apply_patch_parts = 0;

SYSTEM FLUSH LOGS part_log;
-- Both probes above are vacuous under a horizontal merge, which reads every column and so carries
-- the DEFAULT's input anyway. Assert a merge ran and that every one of them was vertical.
SELECT 'merge_algorithm', count() > 0, countIf(merge_algorithm != 'Vertical')
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND database = currentDatabase() AND table = 't_lwu_key_default_merge'
  AND event_type = 'MergeParts';

DROP TABLE t_lwu_key_default_merge SYNC;

-- MATERIALIZED is the other declaration that leaves the column absent from an existing part.
DROP TABLE IF EXISTS t_lwu_key_materialized SYNC;

CREATE TABLE t_lwu_key_materialized (a UInt64, v String, w String)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_key_materialized SELECT number, 'foo', 'w0' FROM numbers(10);

ALTER TABLE t_lwu_key_materialized ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
ALTER TABLE t_lwu_key_materialized MODIFY COLUMN b UInt64 MATERIALIZED length(w);

UPDATE t_lwu_key_materialized SET v = 'bar' WHERE a >= 5;

SELECT 'materialized', countIf(v = 'bar'), countIf(v = 'foo') FROM t_lwu_key_materialized;

DROP TABLE t_lwu_key_materialized SYNC;

-- A wrapped input type must be read and the key expression evaluated over it.
DROP TABLE IF EXISTS t_lwu_key_default_lc SYNC;

CREATE TABLE t_lwu_key_default_lc (a UInt64, v String, w LowCardinality(String))
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_key_default_lc SELECT number, 'foo', 'w0' FROM numbers(10);

ALTER TABLE t_lwu_key_default_lc ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
ALTER TABLE t_lwu_key_default_lc MODIFY COLUMN b UInt64 DEFAULT length(w);

UPDATE t_lwu_key_default_lc SET v = 'bar' WHERE a >= 5;

SELECT 'low_cardinality_input', countIf(v = 'bar'), countIf(v = 'foo') FROM t_lwu_key_default_lc;

DROP TABLE t_lwu_key_default_lc SYNC;
