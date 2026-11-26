-- Coverage for ColumnSparse code paths via ratio_of_defaults_for_sparse_serialization.
-- When the ratio of default (zero/empty) values exceeds the threshold, MergeTree
-- stores the column in sparse format, exercising ColumnSparse read/write paths.

DROP TABLE IF EXISTS t_sparse;

CREATE TABLE t_sparse
(
    id   UInt32,
    val  UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

-- Insert a batch where most values are 0 (default) to trigger sparse encoding.
INSERT INTO t_sparse
SELECT
    number AS id,
    if(number % 20 = 0, number, 0) AS val
FROM numbers(200);

-- Force a merge so sparse serialization is applied.
OPTIMIZE TABLE t_sparse FINAL;

-- Basic read covering sparse deserialization.
SELECT count(), sum(val) FROM t_sparse;

-- Filter on the sparse column to exercise predicate pushdown on sparse data.
SELECT id, val FROM t_sparse WHERE val != 0 ORDER BY id;

DROP TABLE t_sparse;

-- Sparse offsets in compressed blocks of 13 bytes: their VarUInts (1, 2 and 9 bytes) are split between blocks.
DROP TABLE IF EXISTS t_sparse_small_blocks;

CREATE TABLE t_sparse_small_blocks (id UInt32, u UInt64, s String)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    index_granularity = 1000, index_granularity_bytes = '10Mi', min_compress_block_size = 13, max_compress_block_size = 13;

INSERT INTO t_sparse_small_blocks
SELECT
    number,
    if(number % 1000 < 3 OR number % 200 = 100 OR (number % 5000 < 2500 AND number % 50 = 0), number, 0),
    if((number % 7 = 0 AND number % 1000 < 300) OR number % 1000 = 800, toString(number), '')
FROM numbers(30000);

SELECT column, groupUniqArray(serialization_kind), sum(column_data_compressed_bytes) > 2 * sum(column_data_uncompressed_bytes)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_small_blocks' AND active AND column IN ('u', 's')
GROUP BY column ORDER BY column;

SELECT
    countIf(u != if(id % 1000 < 3 OR id % 200 = 100 OR (id % 5000 < 2500 AND id % 50 = 0), id, 0)),
    countIf(s != if((id % 7 = 0 AND id % 1000 < 300) OR id % 1000 = 800, toString(id), '')),
    sum(u), countIf(s != ''), count()
FROM t_sparse_small_blocks
SETTINGS max_block_size = 7, max_threads = 1, use_columns_cache = 0;

SELECT
    countIf(u != if(id % 1000 < 3 OR id % 200 = 100 OR (id % 5000 < 2500 AND id % 50 = 0), id, 0)),
    countIf(s != if((id % 7 = 0 AND id % 1000 < 300) OR id % 1000 = 800, toString(id), '')),
    sum(u), countIf(s != ''), count()
FROM t_sparse_small_blocks
SETTINGS max_block_size = 1000, max_threads = 1, use_columns_cache = 0;

SELECT
    countIf(u != if(id % 1000 < 3 OR id % 200 = 100 OR (id % 5000 < 2500 AND id % 50 = 0), id, 0)),
    countIf(s != if((id % 7 = 0 AND id % 1000 < 300) OR id % 1000 = 800, toString(id), '')),
    sum(u), countIf(s != ''), count()
FROM t_sparse_small_blocks
SETTINGS use_columns_cache = 0;

DROP TABLE t_sparse_small_blocks;
