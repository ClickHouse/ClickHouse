-- The automatic `LowCardinality` encoding of a part is chosen anew every time the part is written,
-- not inherited from the source parts: lowering `max_uniq_number_for_low_cardinality` from one
-- nonzero value to a smaller one demotes an already encoded column on the next merge or rewrite, and
-- a column whose rewritten data qualifies for sparse serialization is written as sparse instead.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;

-- 1) A merge re-evaluates the threshold.
DROP TABLE IF EXISTS t_auto_lc_rechoose_merge;
CREATE TABLE t_auto_lc_rechoose_merge
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_auto_lc_rechoose_merge;

INSERT INTO t_auto_lc_rechoose_merge SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);
INSERT INTO t_auto_lc_rechoose_merge SELECT number, 'w_' || toString(number % 8) FROM numbers(2000);

SELECT 'merge: kind with the high threshold';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rechoose_merge' AND active AND column = 'lc';

-- The merged column has 18 distinct values, which no longer fits under the lowered threshold.
ALTER TABLE t_auto_lc_rechoose_merge MODIFY SETTING max_uniq_number_for_low_cardinality = 5;
SYSTEM START MERGES t_auto_lc_rechoose_merge;
OPTIMIZE TABLE t_auto_lc_rechoose_merge FINAL;

SELECT 'merge: kind after the threshold is lowered, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rechoose_merge' AND active AND column = 'lc';
SELECT count(), uniqExact(lc) FROM t_auto_lc_rechoose_merge;

DROP TABLE t_auto_lc_rechoose_merge;

-- 2) A merge lets sparse serialization win once the merged data qualifies for it.
DROP TABLE IF EXISTS t_auto_lc_rechoose_sparse;
CREATE TABLE t_auto_lc_rechoose_sparse
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_auto_lc_rechoose_sparse;

INSERT INTO t_auto_lc_rechoose_sparse SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'sparse: kind of the dense part';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rechoose_sparse' AND active AND column = 'lc';

-- Mostly default values, so the merged column qualifies for sparse serialization, which takes
-- precedence over the automatic `LowCardinality` encoding.
INSERT INTO t_auto_lc_rechoose_sparse SELECT number, '' FROM numbers(2000, 20000);

SYSTEM START MERGES t_auto_lc_rechoose_sparse;
OPTIMIZE TABLE t_auto_lc_rechoose_sparse FINAL;

SELECT 'sparse: kind of the merged part, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rechoose_sparse' AND active AND column = 'lc';
SELECT count(), uniqExact(lc), countIf(lc = '') FROM t_auto_lc_rechoose_sparse;

DROP TABLE t_auto_lc_rechoose_sparse;

-- 3) A mutation that rewrites the column re-chooses the encoding from the current threshold.
-- Like sparse serialization, the encoding of a mutated part cannot be derived from the data the
-- mutation writes: the writer needs the serialization before the first row is written, and
-- `MergedColumnOnlyOutputStream::fillChecksums` only refreshes the accumulated data afterwards.
-- The following merge is what re-chooses the encoding from the new data.
DROP TABLE IF EXISTS t_auto_lc_rechoose_mutation;
CREATE TABLE t_auto_lc_rechoose_mutation
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc_rechoose_mutation SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'mutation: kind before the rewrite';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rechoose_mutation' AND active AND column = 'lc';

-- The rewritten column has 10 distinct values, which no longer fits under the lowered threshold.
ALTER TABLE t_auto_lc_rechoose_mutation MODIFY SETTING max_uniq_number_for_low_cardinality = 5;
ALTER TABLE t_auto_lc_rechoose_mutation UPDATE lc = 'w_' || toString(id % 10) WHERE 1;

SELECT 'mutation: kind after the rewrite, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rechoose_mutation' AND active AND column = 'lc';
SELECT count(), uniqExact(lc) FROM t_auto_lc_rechoose_mutation;

DROP TABLE t_auto_lc_rechoose_mutation;

-- 4) A mutation that makes every value the default one keeps the encoding of the part it writes, and
-- the next merge switches it to sparse serialization.
DROP TABLE IF EXISTS t_auto_lc_rechoose_mutation_sparse;
CREATE TABLE t_auto_lc_rechoose_mutation_sparse
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc_rechoose_mutation_sparse SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);
ALTER TABLE t_auto_lc_rechoose_mutation_sparse UPDATE lc = '' WHERE 1;

SELECT 'mutation to defaults: kind after the rewrite';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rechoose_mutation_sparse' AND active AND column = 'lc';

OPTIMIZE TABLE t_auto_lc_rechoose_mutation_sparse FINAL;

SELECT 'mutation to defaults: kind after the following merge, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rechoose_mutation_sparse' AND active AND column = 'lc';
SELECT count(), uniqExact(lc), countIf(lc = '') FROM t_auto_lc_rechoose_mutation_sparse;

DROP TABLE t_auto_lc_rechoose_mutation_sparse;
