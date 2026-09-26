-- Regression test: with `patch_parts_version = 'v1'` a lightweight UPDATE that covers rows of several
-- data parts was not applied to the first row of a data part whose patch rows begin in a later granule
-- of the patch part. The result depended on the read task and block size, so both spellings must agree.

DROP TABLE IF EXISTS t_lwu_span_parts;

CREATE TABLE t_lwu_span_parts (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         enable_block_number_column = 1, enable_block_offset_column = 1,
         patch_parts_version = 'v1',
         index_granularity = 4, index_granularity_bytes = 0,
         remove_unused_patch_parts = 0;

SYSTEM STOP MERGES t_lwu_span_parts;

INSERT INTO t_lwu_span_parts SELECT number, 0 FROM numbers(10);
INSERT INTO t_lwu_span_parts SELECT number + 10, 0 FROM numbers(10);

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

UPDATE t_lwu_span_parts SET v = 1 WHERE id < 4 OR id >= 10;

-- One patch part holding 4 rows of the first data part and 10 of the second, so its first granule
-- carries nothing for the second one.
SELECT count(), sum(rows) FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_span_parts' AND active AND startsWith(name, 'patch');

SELECT sum(v) FROM t_lwu_span_parts;
SELECT sum(v) FROM t_lwu_span_parts SETTINGS merge_tree_min_read_task_size = 1, max_block_size = 1;

DROP TABLE t_lwu_span_parts;

-- A patch part written with non-adaptive granularity has no final mark, so its index does not bound its
-- last granule from above. A data part whose patch rows begin in that granule was not patched at all,
-- and the loss also survived materialization of the patch part.

DROP TABLE IF EXISTS t_lwu_patch_last_mark;

CREATE TABLE t_lwu_patch_last_mark (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         enable_block_number_column = 1, enable_block_offset_column = 1,
         patch_parts_version = 'v1',
         index_granularity = 3, index_granularity_bytes = 0,
         remove_unused_patch_parts = 0;

SYSTEM STOP MERGES t_lwu_patch_last_mark;

INSERT INTO t_lwu_patch_last_mark SELECT number, 0 FROM numbers(5);
INSERT INTO t_lwu_patch_last_mark SELECT number + 5, 0 FROM numbers(2);

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

UPDATE t_lwu_patch_last_mark SET v = 1 WHERE id >= 1;

-- One patch part of 6 rows in 2 granules, so the second data part's rows start inside the last one.
SELECT count(), sum(rows) FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_patch_last_mark' AND active AND startsWith(name, 'patch');

SELECT sum(v) FROM t_lwu_patch_last_mark;

-- Materializing the patch part must not lose the updates either. The mutation is executed by the
-- background merge pool, so merges have to be enabled again for it to run.
SYSTEM START MERGES t_lwu_patch_last_mark;
ALTER TABLE t_lwu_patch_last_mark APPLY PATCHES SETTINGS mutations_sync = 2;
SELECT sum(v) FROM t_lwu_patch_last_mark SETTINGS apply_patch_parts = 0;

DROP TABLE t_lwu_patch_last_mark;
