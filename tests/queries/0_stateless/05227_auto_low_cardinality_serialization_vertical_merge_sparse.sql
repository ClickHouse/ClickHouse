-- A vertical merge gathers each column separately, so the column of the merged part is built by
-- inserting the columns read from the source parts into one another. Parts of the same table can store
-- a column with automatic `LowCardinality` serialization, as sparse, or plain, and the merged part can
-- pick any of the three from the combined statistics: every combination of source and result
-- representations has to be written correctly.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

-- 1) Encoded part first, sparse-qualifying part second, sparse wins in the merged part.
DROP TABLE IF EXISTS t_auto_lc_vertical_sparse;
CREATE TABLE t_auto_lc_vertical_sparse
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    min_bytes_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_auto_lc_vertical_sparse;

INSERT INTO t_auto_lc_vertical_sparse SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);
INSERT INTO t_auto_lc_vertical_sparse SELECT number, '' FROM numbers(2000, 20000);

SELECT 'encoded then sparse: kinds of the source parts';
SELECT name, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_vertical_sparse' AND active AND column = 'lc'
ORDER BY name;

SYSTEM START MERGES t_auto_lc_vertical_sparse;
OPTIMIZE TABLE t_auto_lc_vertical_sparse FINAL;

SELECT 'encoded then sparse: kind of the merged part, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_vertical_sparse' AND active AND column = 'lc';
SELECT count(), uniqExact(lc), countIf(lc = ''), sum(length(lc)) FROM t_auto_lc_vertical_sparse;

DROP TABLE t_auto_lc_vertical_sparse;

-- 2) Sparse-qualifying part first, encoded part second: the merged column is built from the first
-- source, so the order of the parts must not matter.
DROP TABLE IF EXISTS t_auto_lc_vertical_sparse_first;
CREATE TABLE t_auto_lc_vertical_sparse_first
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    min_bytes_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_auto_lc_vertical_sparse_first;

INSERT INTO t_auto_lc_vertical_sparse_first SELECT number, '' FROM numbers(20000);
INSERT INTO t_auto_lc_vertical_sparse_first SELECT number, 'v_' || toString(number % 10) FROM numbers(20000, 2000);

SELECT 'sparse then encoded: kinds of the source parts';
SELECT name, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_vertical_sparse_first' AND active AND column = 'lc'
ORDER BY name;

SYSTEM START MERGES t_auto_lc_vertical_sparse_first;
OPTIMIZE TABLE t_auto_lc_vertical_sparse_first FINAL;

SELECT 'sparse then encoded: kind of the merged part, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_vertical_sparse_first' AND active AND column = 'lc';
SELECT count(), uniqExact(lc), countIf(lc = ''), sum(length(lc)) FROM t_auto_lc_vertical_sparse_first;

DROP TABLE t_auto_lc_vertical_sparse_first;

-- 3) A sparse part and an encoded part whose combination is dense enough for the encoding to win.
DROP TABLE IF EXISTS t_auto_lc_vertical_lc_wins;
CREATE TABLE t_auto_lc_vertical_lc_wins
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    min_bytes_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_auto_lc_vertical_lc_wins;

INSERT INTO t_auto_lc_vertical_lc_wins SELECT number, '' FROM numbers(2000);
INSERT INTO t_auto_lc_vertical_lc_wins SELECT number, 'v_' || toString(number % 10) FROM numbers(2000, 20000);

SELECT 'sparse then encoded, encoding wins: kinds of the source parts';
SELECT name, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_vertical_lc_wins' AND active AND column = 'lc'
ORDER BY name;

SYSTEM START MERGES t_auto_lc_vertical_lc_wins;
OPTIMIZE TABLE t_auto_lc_vertical_lc_wins FINAL;

SELECT 'sparse then encoded, encoding wins: kind of the merged part, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_vertical_lc_wins' AND active AND column = 'lc';
SELECT count(), uniqExact(lc), countIf(lc = ''), sum(length(lc)) FROM t_auto_lc_vertical_lc_wins;

DROP TABLE t_auto_lc_vertical_lc_wins;
