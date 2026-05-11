-- Tags: no-fasttest

-- Smoke-test the DDL validator + guards for the table-level DiskANN index.
-- The tests in this file only exercise CREATE/ALTER validation: no data is inserted, so
-- background build and search are never invoked.

DROP TABLE IF EXISTS t_ann_ok;
DROP TABLE IF EXISTS t_ann_add;

-- --- Normal CREATE with both `_block_number` / `_block_offset` columns enabled ---
CREATE TABLE t_ann_ok
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

-- --- Missing `_block_number` column: rejected ---
CREATE TABLE t_ann_no_block_col
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_offset_column = 1; -- { serverError SUPPORT_IS_DISABLED }

-- --- Missing `_block_offset` column: rejected ---
CREATE TABLE t_ann_no_block_col
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1; -- { serverError SUPPORT_IS_DISABLED }

-- --- Wrong vector type: rejected ---
CREATE TABLE t_ann_wrong_type
(
    id UInt64,
    emb Array(Float64),
    INDEX idx_emb emb TYPE ann(dim = 4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError ILLEGAL_COLUMN }

-- --- Missing `dim`: rejected ---
CREATE TABLE t_ann_no_dim
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(metric = 'L2')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- Unknown algorithm: rejected ---
CREATE TABLE t_ann_unknown_algo
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(algorithm = 'hnsw', dim = 4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- Mixing `ann` and `vector_similarity` on the same column: rejected ---
CREATE TABLE t_ann_mixed
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_ann emb TYPE ann(dim = 4),
    INDEX idx_vs  emb TYPE vector_similarity('hnsw', 'L2Distance', 4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- MODIFY SETTING cannot turn off the block columns while the ANN index is present ---
ALTER TABLE t_ann_ok MODIFY SETTING enable_block_number_column = 0; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_ann_ok MODIFY SETTING enable_block_offset_column = 0; -- { serverError SUPPORT_IS_DISABLED }

-- --- DROP INDEX removes the guard; afterwards the setting can be flipped ---
ALTER TABLE t_ann_ok DROP INDEX idx_emb;
ALTER TABLE t_ann_ok MODIFY SETTING enable_block_number_column = 0;

-- --- ALTER ADD INDEX on a table that already has the prerequisites ---
CREATE TABLE t_ann_add
(
    id UInt64,
    emb Array(Float32)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

ALTER TABLE t_ann_add ADD INDEX idx_emb emb TYPE ann(dim = 4) GRANULARITY 1;

-- --- Changing the `dim` via DROP + ADD is accepted (shape change path) ---
ALTER TABLE t_ann_add DROP INDEX idx_emb;
ALTER TABLE t_ann_add ADD INDEX idx_emb emb TYPE ann(dim = 8) GRANULARITY 1;

DROP TABLE t_ann_ok;
DROP TABLE t_ann_add;
