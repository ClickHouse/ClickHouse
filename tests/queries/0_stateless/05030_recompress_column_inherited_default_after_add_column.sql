SET check_query_single_value_result = 1;

-- A wide part that predates an unrelated `ADD COLUMN` stores fewer columns than the table metadata
-- has. `RECOMPRESS COLUMN` of a column that inherits the table-wide `default_compression_codec` needs
-- the whole-part rewrite (the in-place path can only resolve `Default` against the part's stored
-- default codec, which does not follow a `MODIFY SETTING default_compression_codec`). The task
-- selection must follow that decision explicitly: deriving it from the interpreter's output header
-- routed such a part to the per-column path, which keeps the source part's default codec and made
-- the recompression a silent no-op.
DROP TABLE IF EXISTS t_recompress_inherited_after_add;

CREATE TABLE t_recompress_inherited_after_add (id UInt64, x String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    default_compression_codec = 'NONE';

INSERT INTO t_recompress_inherited_after_add SELECT number, repeat('a', 100) FROM numbers(100000);

SELECT DISTINCT 'part type', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_inherited_after_add' AND active;

SELECT 'none is large', sum(data_compressed_bytes) > 5000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_inherited_after_add' AND column = 'x' AND active;

-- The existing part has no stream for `y`, so the mutation's read set is a strict subset of the
-- table's physical columns.
ALTER TABLE t_recompress_inherited_after_add ADD COLUMN y UInt8 DEFAULT 7;

ALTER TABLE t_recompress_inherited_after_add MODIFY SETTING default_compression_codec = 'ZSTD';
ALTER TABLE t_recompress_inherited_after_add RECOMPRESS COLUMN x SETTINGS mutations_sync = 2;

SELECT 'after', count(), countIf(x = repeat('a', 100)), countIf(y = 7) FROM t_recompress_inherited_after_add;
SELECT 'zstd is small', sum(data_compressed_bytes) < 1000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_inherited_after_add' AND column = 'x' AND active;

CHECK TABLE t_recompress_inherited_after_add;

DROP TABLE t_recompress_inherited_after_add;
