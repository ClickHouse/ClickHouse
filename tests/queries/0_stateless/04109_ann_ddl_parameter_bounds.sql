-- Tags: no-fasttest

-- DDL-time parameter validation for the `ann` index.
-- Complements 04102 (structural guards + required args) by pinning down the
-- value-level rules actually enforced during CREATE/ALTER: `dim` range,
-- `metric` whitelist, duplicate / unknown / positional / type-mismatched
-- arguments. No data is inserted, so build and search never run.
--
-- Numeric parameters that the validator *accepts without bounds* today
-- (`max_degree`, `build_search_list_size`, `alpha`, `pq_chunks`, ...) are
-- deliberately not tested here: the current behaviour is "DDL passes, FFI
-- may fail later", which belongs to build-time tests, not DDL tests.

DROP TABLE IF EXISTS t_ann_bounds;

-- --- `dim = 0` rejected ---
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- `dim` just above the supported upper bound (16384) rejected ---
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 16385)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- Unsupported `metric` values rejected (whitelist: L2, Cosine) ---
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4, metric = 'Hamming')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- The whitelist is case-sensitive.
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4, metric = 'l2')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- Unknown argument name rejected ---
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4, not_a_real_param = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- Duplicate argument rejected ---
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4, dim = 8)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- Positional (non key=value) arguments rejected ---
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(4)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- Type-mismatched value rejected (string where UInt is required) ---
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 'four')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- A string literal where a numeric argument is expected is rejected even when
-- `dim` itself is valid.
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4, alpha = 'high')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- ALTER ADD INDEX path enforces the same rules ---
CREATE TABLE t_ann_bounds
(
    id UInt64,
    emb Array(Float32)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

ALTER TABLE t_ann_bounds ADD INDEX idx_bad emb TYPE ann(dim = 0); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_ann_bounds ADD INDEX idx_bad emb TYPE ann(dim = 4, metric = 'Hamming'); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_ann_bounds ADD INDEX idx_bad emb TYPE ann(dim = 4, bogus = 1); -- { serverError BAD_ARGUMENTS }

-- Sanity: a well-formed ALTER ADD still succeeds.
ALTER TABLE t_ann_bounds ADD INDEX idx_ok emb TYPE ann(dim = 4, metric = 'Cosine') GRANULARITY 1;

DROP TABLE t_ann_bounds;
