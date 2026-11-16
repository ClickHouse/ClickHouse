-- Tags: no-parallel-replicas
-- (the two-stage quantized codes rewrite is disabled under parallel replicas, so the lazy read of the vectors does not happen.)

-- With `merge_tree_use_one_row_per_block_layout`, the second stage of lazy materialization reads only the shortlisted rows
-- of a vector column with the column-level setting `uncompressed_fixed_array_size`, not whole granules.
-- The results must be the same as with the usual readers.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET vector_search_index_fetch_multiplier = 10;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;
-- The setting has no effect with the columns cache or the uncompressed cache.
SET use_columns_cache = 0;
SET use_uncompressed_cache = 0;

DROP TABLE IF EXISTS tab_one_row_per_block;
DROP TABLE IF EXISTS tab_several_rows_per_block;
DROP TABLE IF EXISTS tab_adaptive;
DROP TABLE IF EXISTS tab_compact;
DROP TABLE IF EXISTS tab_lightweight;
DROP TABLE IF EXISTS tab_merged;
DROP TABLE IF EXISTS tab_rejected;
DROP TABLE IF EXISTS tab_alter;
DROP TABLE IF EXISTS tab_without_setting;
DROP TABLE IF EXISTS tab_same_setting;

-- Every row of 64 `Float32` elements is one block of 256 bytes, whatever the table-level block sizes.
CREATE TABLE tab_one_row_per_block
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (uncompressed_fixed_array_size = 64)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    index_granularity = 8192, index_granularity_bytes = 10485760;

SYSTEM STOP MERGES tab_one_row_per_block;

INSERT INTO tab_one_row_per_block SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000);

SELECT 'one part, setting on';
WITH (SELECT vec FROM tab_one_row_per_block WHERE id = 123) AS ref
SELECT id FROM tab_one_row_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_one_part_on';

SELECT 'one part, setting off';
WITH (SELECT vec FROM tab_one_row_per_block WHERE id = 123) AS ref
SELECT id FROM tab_one_row_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, log_comment = 'one_row_per_block_one_part_off';

-- The setting applies only to the stream of the array elements of a `Quantized` column. The quantized
-- codes, 68 bytes per row, are stored in blocks of the default size, not in a block per 4 rows. The number of blocks
-- decompressed is counted, so the read is not split.
SELECT count() FROM tab_one_row_per_block WHERE NOT ignore(vec.quantized)
SETTINGS log_comment = 'one_row_per_block_codes', max_threads = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- A Compact part is read with the usual readers. The rows of both parts must keep their order.
ALTER TABLE tab_one_row_per_block MODIFY SETTING min_bytes_for_wide_part = 1000000000;
INSERT INTO tab_one_row_per_block SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000, 2000);

SELECT 'two parts, setting on';
WITH (SELECT vec FROM tab_one_row_per_block WHERE id = 123) AS ref
SELECT id FROM tab_one_row_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_two_parts_on';

SELECT 'two parts, setting off';
WITH (SELECT vec FROM tab_one_row_per_block WHERE id = 123) AS ref
SELECT id FROM tab_one_row_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, log_comment = 'one_row_per_block_two_parts_off';

-- Without the setting, the blocks hold several rows, and the usual readers are used.
CREATE TABLE tab_several_rows_per_block
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_several_rows_per_block SELECT * FROM tab_one_row_per_block WHERE id < 2000;

SELECT 'several rows per block, setting on';
WITH (SELECT vec FROM tab_several_rows_per_block WHERE id = 123) AS ref
SELECT id FROM tab_several_rows_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_several_rows_per_block_on';

-- The queries below are sorted by an expression other than the sorting key, so that the vectors are read lazily.
SELECT 'same sizes, setting on';
SELECT id, length(vec), vec[1] FROM tab_one_row_per_block ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_same_sizes_on';

SELECT 'show create';
SELECT replaceAll(replaceRegexpOne(create_table_query, 'ENGINE.*', ''), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'tab_one_row_per_block';

-- The adaptive write buffer and the table-level block sizes do not apply to the column.
CREATE TABLE tab_adaptive
(
    id UInt32,
    vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 800)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 2, index_granularity_bytes = 0, min_compress_block_size = 1,
    max_compress_block_size = 100, min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 100;

INSERT INTO tab_adaptive SELECT number, arrayMap(x -> toFloat32(number), range(800)) FROM numbers(12);

-- A row of another number of elements, or without the column, which is then filled with an empty array, is rejected.
INSERT INTO tab_adaptive SELECT number, arrayMap(j -> toFloat32(j), range(799)) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
INSERT INTO tab_adaptive (id) VALUES (1); -- { serverError BAD_ARGUMENTS }
-- Also into a Compact part, so that its merges into a Wide part do not fail later.
CREATE TABLE tab_compact (id UInt32, vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 2))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO tab_compact VALUES (1, [1]); -- { serverError BAD_ARGUMENTS }
INSERT INTO tab_compact VALUES (1, [1, 2]);
SELECT 'compact part', part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 'tab_compact' AND active;
DROP TABLE tab_compact;
SELECT 'rows after rejected inserts', count() FROM tab_adaptive;

SELECT 'adaptive write buffer, setting on';
SELECT id, vec[1], vec[-1], length(vec) FROM tab_adaptive ORDER BY abs(toInt64(id) - 1) LIMIT 1
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_adaptive_on';

SELECT 'adaptive write buffer, setting off';
SELECT id, vec[1], vec[-1], length(vec) FROM tab_adaptive ORDER BY abs(toInt64(id) - 1) LIMIT 1
SETTINGS merge_tree_use_one_row_per_block_layout = 0;

-- A table with a default expression with an identifier is read as usual, even if every part has the column.
ALTER TABLE tab_adaptive ADD COLUMN m UInt64 MATERIALIZED id * 2;
ALTER TABLE tab_adaptive MATERIALIZE COLUMN m SETTINGS mutations_sync = 2;

SELECT 'materialized column, setting on';
SELECT id, m, vec[1], length(vec) FROM tab_adaptive ORDER BY abs(toInt64(id) - 1) LIMIT 1
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_materialized_on';

-- Horizontal and vertical merges, a mutation of another column, which keeps the file of the vectors, and a mutation of
-- the vectors write every row as one block as well.
CREATE TABLE tab_merged
(
    id UInt32,
    vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 64),
    x UInt8
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_merged SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), 0
FROM numbers(1000);
INSERT INTO tab_merged SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), 0
FROM numbers(1000, 1000);
OPTIMIZE TABLE tab_merged FINAL;

SELECT 'merged part, setting on';
SELECT id, vec[1] FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_merged_on';

ALTER TABLE tab_merged MODIFY SETTING vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;
INSERT INTO tab_merged SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), 0
FROM numbers(2000, 1000);
OPTIMIZE TABLE tab_merged FINAL;

SYSTEM FLUSH LOGS part_log;
SELECT 'vertical merge', merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 'tab_merged' AND event_type = 'MergeParts' ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'vertically merged part, setting on';
SELECT id, vec[1] FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_vertically_merged_on';

ALTER TABLE tab_merged UPDATE x = 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'mutated part, setting on';
SELECT id, vec[1], x FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_mutated_on';

ALTER TABLE tab_merged UPDATE vec = arrayMap(v -> v + 1, vec) WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'mutated vectors, setting on';
SELECT id, vec[1], x FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_vec_mutated_on';

SELECT 'mutated vectors, setting off';
SELECT id, vec[1], x FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 0;

-- A rename keeps the setting.
ALTER TABLE tab_merged RENAME COLUMN vec TO vec2 SETTINGS mutations_sync = 2;

SELECT 'renamed column, setting on';
SELECT id, vec2[1] FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_renamed_on';

-- The columns that cannot have the setting.
CREATE TABLE tab_rejected (id UInt32, vec Array(UInt32) SETTINGS (uncompressed_fixed_array_size = 64)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_rejected (id UInt32, vec String SETTINGS (uncompressed_fixed_array_size = 64)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_rejected (id UInt32, vec Array(Array(Float32)) SETTINGS (uncompressed_fixed_array_size = 64)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_rejected (id UInt32, vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 0)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_rejected (id UInt32, vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 100000000)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_rejected (id UInt32, vec Array(Float32) CODEC(LZ4) SETTINGS (uncompressed_fixed_array_size = 64)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_rejected (id UInt32, vec Array(Float32) CODEC(NONE, LZ4) SETTINGS (uncompressed_fixed_array_size = 64)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
-- A chain of codecs adds a header to every block, even of codecs that store the data as is.
CREATE TABLE tab_rejected (id UInt32, vec Array(Float32) CODEC(NONE, NONE) SETTINGS (uncompressed_fixed_array_size = 64)) ENGINE = MergeTree ORDER BY id SETTINGS allow_suspicious_codecs = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_rejected (id UInt32, vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 64, max_compress_block_size = 256)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_rejected (id UInt32, vec Array(Float32) ALIAS [toFloat32(id)] SETTINGS (uncompressed_fixed_array_size = 1)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
-- An expired TTL would replace the values with the default.
CREATE TABLE tab_rejected (id UInt32, d DateTime, vec Array(Float32) TTL d + INTERVAL 1 DAY SETTINGS (uncompressed_fixed_array_size = 1)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- At table level, the setting has no effect.
CREATE TABLE tab_rejected (id UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id SETTINGS uncompressed_fixed_array_size = 64;
DROP TABLE tab_rejected;

-- `ALTER` can neither add, change nor remove the setting, nor change the type or the codec of a column with it.
CREATE TABLE tab_alter
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (uncompressed_fixed_array_size = 64),
    other Array(Float32)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

ALTER TABLE tab_alter ADD COLUMN vec3 Array(Float32) SETTINGS (uncompressed_fixed_array_size = 64); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN other MODIFY SETTING uncompressed_fixed_array_size = 64; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN other Array(Float32) SETTINGS (uncompressed_fixed_array_size = 64); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec MODIFY SETTING uncompressed_fixed_array_size = 32; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec RESET SETTING uncompressed_fixed_array_size; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec MODIFY SETTING max_compress_block_size = 1024; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec REMOVE SETTINGS; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec Array(Float64); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec Array(Float32) CODEC(LZ4); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec Array(Float32) CODEC(NONE); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec REMOVE CODEC; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec Array(Float32) ALIAS [toFloat32(id)]; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter MODIFY COLUMN vec TTL toDateTime(id) + INTERVAL 1 DAY; -- { serverError BAD_ARGUMENTS }
-- A clear would replace the values with the default, and a pending clear makes the reader fill the column from it.
ALTER TABLE tab_alter CLEAR COLUMN vec; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter CLEAR COLUMN vec IN PARTITION tuple(); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_alter CLEAR COLUMN other;
-- Restating the type, or changing the comment, keeps how the column is stored.
ALTER TABLE tab_alter MODIFY COLUMN vec Array(Float32);
ALTER TABLE tab_alter MODIFY COLUMN vec COMMENT 'vectors';
SELECT 'after alter';
SELECT name, compression_codec, comment FROM system.columns WHERE database = currentDatabase() AND table = 'tab_alter' AND name = 'vec';

-- The operations that bring in parts that the table did not write are not supported: partitions cannot be moved between
-- tables, even with the same setting, and parts cannot be attached from the detached directory.
CREATE TABLE tab_without_setting (id UInt32, vec Array(Float32) CODEC(NONE), other Array(Float32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
CREATE TABLE tab_same_setting AS tab_alter;
INSERT INTO tab_without_setting SELECT number, arrayMap(j -> toFloat32(j), range(64)), [] FROM numbers(10);
INSERT INTO tab_same_setting SELECT number, arrayMap(j -> toFloat32(j), range(64)), [] FROM numbers(10);
ALTER TABLE tab_alter ATTACH PARTITION tuple() FROM tab_without_setting; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE tab_alter REPLACE PARTITION tuple() FROM tab_without_setting; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE tab_without_setting ATTACH PARTITION tuple() FROM tab_alter; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE tab_alter ATTACH PARTITION tuple() FROM tab_same_setting; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE tab_same_setting MOVE PARTITION tuple() TO TABLE tab_alter; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE tab_same_setting DETACH PARTITION tuple();
ALTER TABLE tab_same_setting ATTACH PARTITION tuple(); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE tab_same_setting ATTACH PART 'all_1_1_0'; -- { serverError NOT_IMPLEMENTED }
SELECT 'rows after rejected imports', (SELECT count() FROM tab_alter), (SELECT count() FROM tab_same_setting);
DROP TABLE tab_same_setting;

-- A lightweight update writes a patch part, whose values are checked against the setting of the table as well.
CREATE TABLE tab_lightweight (id UInt32, vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 2))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO tab_lightweight VALUES (1, [1, 2]), (2, [3, 4]);
UPDATE tab_lightweight SET vec = [3] WHERE id = 1 SETTINGS enable_lightweight_update = 1; -- { serverError BAD_ARGUMENTS }
UPDATE tab_lightweight SET vec = [5, 6] WHERE id = 1 SETTINGS enable_lightweight_update = 1;
OPTIMIZE TABLE tab_lightweight FINAL;
SELECT 'lightweight update', id, vec FROM tab_lightweight ORDER BY id;
DROP TABLE tab_lightweight;


SYSTEM FLUSH LOGS query_log;

SELECT 'codes in few blocks', ProfileEvents['CompressedReadBufferBlocks'] < 10
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'one_row_per_block_codes';

-- The shortlist has 10 * 10 = 100 rows. With one Wide part, all of them are read this way.
-- With a second part that is Compact, only the rows of the first part are. Without the setting or with the query setting
-- off, none are. The queries sorted by `sipHash64(id)` read the 5 rows of their result.
SELECT log_comment, ProfileEvents['MergeTreeOneRowPerBlockReadRows'] AS rows_read_by_blocks
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('one_row_per_block_one_part_on', 'one_row_per_block_one_part_off', 'one_row_per_block_two_parts_on', 'one_row_per_block_two_parts_off', 'one_row_per_block_several_rows_per_block_on',
        'one_row_per_block_same_sizes_on', 'one_row_per_block_adaptive_on', 'one_row_per_block_materialized_on', 'one_row_per_block_merged_on', 'one_row_per_block_vertically_merged_on', 'one_row_per_block_mutated_on', 'one_row_per_block_vec_mutated_on',
        'one_row_per_block_renamed_on')
ORDER BY log_comment;

DROP TABLE tab_one_row_per_block;
DROP TABLE tab_several_rows_per_block;
DROP TABLE tab_adaptive;
DROP TABLE tab_merged;
DROP TABLE tab_alter;
DROP TABLE tab_without_setting;
