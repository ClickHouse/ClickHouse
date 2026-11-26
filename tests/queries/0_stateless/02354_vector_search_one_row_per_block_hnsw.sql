-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-fasttest: the vector similarity index is not in the fast test build.
-- no-parallel-replicas: the vector search optimization, and with it the candidate rows, is disabled under parallel replicas.

-- With `merge_tree_use_one_row_per_block_layout` and `vector_search_with_rescoring`, only the candidate rows of the vector
-- similarity index are read from a vector column with the setting `uncompressed_fixed_array_size`, not whole granules.
-- The results must be the same as with the usual readers.

SET vector_search_with_rescoring = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
-- The setting has no effect with the columns cache or the uncompressed cache.
SET use_columns_cache = 0;
SET use_uncompressed_cache = 0;
-- Enough candidates (`LIMIT 10` * 100, at most 1000, so all 500 rows) that rescoring always finds the exact nearest
-- neighbours, whatever the approximation of the index.
SET vector_search_index_fetch_multiplier = 100;
SET hnsw_candidate_list_size_for_search = 2048;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_prefetch;
DROP TABLE IF EXISTS tab_dotted;

-- Every row of 64 `Float32` elements is one block. The tables are small, because the thread fuzzer of CI
-- makes every lock taken while building the indexes slow.
CREATE TABLE tab
(
    id UInt32,
    vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 64),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 64, 'b1', 64, 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;

INSERT INTO tab SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(500);

SELECT 'setting on';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = 'one_row_per_block_hnsw_on';

SELECT 'brute force';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS use_skip_indexes = 0;

SELECT 'setting off';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, log_comment = 'one_row_per_block_hnsw_off';

-- Columns that the part does not have are filled after the read, before the candidates are filtered. Without a default
-- expression, or with one that names no column, they do not depend on the placeholder values of the other rows, so only
-- the candidates are read.
ALTER TABLE tab ADD COLUMN e UInt64;
ALTER TABLE tab ADD COLUMN ts DateTime DEFAULT now();

SELECT 'defaults without columns, setting on';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id, e, ts > 0 FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = 'one_row_per_block_hnsw_no_column_defaults_on';

-- A default expression with an identifier, which could name the vector column in many ways, e.g. `dummy.vec`, could use
-- the placeholder values, so from the next query on, the vectors are read as usual. This holds for a default expression
-- of any column, in any part, whether or not the query reads the column.
ALTER TABLE tab ADD COLUMN d UInt64 DEFAULT intDiv(64, length(dummy.vec));

SELECT 'default with an identifier, setting on';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id, d FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = 'one_row_per_block_hnsw_default_on';

SELECT 'default with an identifier, setting off';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id, d FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, query_plan_optimize_lazy_materialization = 0;

SELECT 'default with an identifier, column not read, setting on';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = 'one_row_per_block_hnsw_default_not_read_on';

-- With few prefetches allowed, a reader prefetched for one task is reused for the next tasks, and must read past the end
-- of the range of the first task. The query must not fail; which rows the approximate index returns is not checked.
CREATE TABLE tab_prefetch
(
    id UInt32,
    vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = 64),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 64, 'b1', 64, 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;

INSERT INTO tab_prefetch SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(4000);

SELECT 'reused reader';
WITH (SELECT vec FROM tab_prefetch WHERE id = 123) AS ref
SELECT 'ok' FROM tab_prefetch ORDER BY cosineDistance(vec, ref) LIMIT 3
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, max_threads = 2,
    allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_prefetch = 1, local_filesystem_read_method = 'pread_threadpool',
    filesystem_prefetch_step_marks = 1, filesystem_prefetches_limit = 2, merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read = 0, use_reader_executor = 1;

-- A column with a dot in its name. The indexed column is read as usual, and the column `a.b` only for the candidates.
CREATE TABLE tab_dotted
(
    id UInt32,
    vec Array(Float32),
    `a.b` Array(Float32) SETTINGS (uncompressed_fixed_array_size = 64),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 64, 'b1', 64, 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128, share_nested_offsets = 0;

INSERT INTO tab_dotted SELECT id, vec, vec FROM tab;

SELECT 'dotted column, setting on';
WITH (SELECT vec FROM tab_dotted WHERE id = 123) AS ref
SELECT id, `a.b`[1] FROM tab_dotted ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = 'one_row_per_block_hnsw_dotted_on';

SYSTEM FLUSH LOGS query_log;

-- Only the candidates of the index are read this way, and none with the setting off or a default expression with an
-- identifier.
SELECT log_comment, ProfileEvents['MergeTreeOneRowPerBlockReadRows'] > 0 AS read_by_blocks
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('one_row_per_block_hnsw_on', 'one_row_per_block_hnsw_off', 'one_row_per_block_hnsw_no_column_defaults_on', 'one_row_per_block_hnsw_default_on', 'one_row_per_block_hnsw_default_not_read_on', 'one_row_per_block_hnsw_dotted_on')
ORDER BY log_comment;

DROP TABLE tab;
DROP TABLE tab_prefetch;
DROP TABLE tab_dotted;
