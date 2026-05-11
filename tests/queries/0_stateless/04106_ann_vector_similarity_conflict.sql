-- Tags: no-fasttest

-- DEV-26: reject metadata where the same column carries both a table-level `ann` index and a
-- per-granule `vector_similarity` index.

DROP TABLE IF EXISTS t_conflict_create;
DROP TABLE IF EXISTS t_conflict_a;
DROP TABLE IF EXISTS t_conflict_b;
DROP TABLE IF EXISTS t_conflict_diff;

-- --- CREATE TABLE with both indexes on the same column: rejected ---
CREATE TABLE t_conflict_create
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_ann emb TYPE ann(dim = 4) GRANULARITY 1,
    INDEX idx_vs  emb TYPE vector_similarity('hnsw', 'L2Distance', 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- --- ALTER ADD `ann` on top of an existing `vector_similarity` on the same column: rejected ---
CREATE TABLE t_conflict_a
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_vs emb TYPE vector_similarity('hnsw', 'L2Distance', 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

ALTER TABLE t_conflict_a ADD INDEX idx_ann emb TYPE ann(dim = 4) GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

-- --- ALTER ADD `vector_similarity` on top of an existing `ann` on the same column: rejected ---
CREATE TABLE t_conflict_b
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_ann emb TYPE ann(dim = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

ALTER TABLE t_conflict_b ADD INDEX idx_vs emb TYPE vector_similarity('hnsw', 'L2Distance', 4) GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

-- --- Different columns may carry the two index kinds simultaneously ---
CREATE TABLE t_conflict_diff
(
    id UInt64,
    emb_a Array(Float32),
    emb_b Array(Float32),
    INDEX idx_ann emb_a TYPE ann(dim = 4) GRANULARITY 1,
    INDEX idx_vs  emb_b TYPE vector_similarity('hnsw', 'L2Distance', 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_conflict_diff';

DROP TABLE t_conflict_a;
DROP TABLE t_conflict_b;
DROP TABLE t_conflict_diff;

-- --- SYSTEM BUILD ANN INDEX on a MergeTree table without an `ann` index: rejected ---
DROP TABLE IF EXISTS t_no_ann;
CREATE TABLE t_no_ann
(
    id UInt64,
    emb Array(Float32)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM BUILD ANN INDEX t_no_ann; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_no_ann;

-- --- SYSTEM BUILD ANN INDEX on a non-MergeTree engine: rejected ---
DROP TABLE IF EXISTS t_memory_ann;
CREATE TABLE t_memory_ann
(
    id UInt64,
    emb Array(Float32)
)
ENGINE = Memory;

SYSTEM BUILD ANN INDEX t_memory_ann; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_memory_ann;
