-- Regression tests for automatic LowCardinality serialization on write paths beyond the initial INSERT:
-- vertical merge (the encoded column is written by `MergedColumnOnlyOutputStream`), the encoded column
-- being part of `ORDER BY` with unsorted input (it must be written from the dictionary-encoded column,
-- not from the materialized primary key column), an `ALTER` that changes the column type away from
-- String (the LowCardinality kind must be dropped), and a mutation that rewrites the encoded column.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;

-- 1) Vertical merge: a non-key encoded column is written by `MergedColumnOnlyOutputStream`.
DROP TABLE IF EXISTS t_auto_lc_vmerge;
CREATE TABLE t_auto_lc_vmerge
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_auto_lc_vmerge SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);
INSERT INTO t_auto_lc_vmerge SELECT number, 'w_' || toString(number % 8) FROM numbers(2000);

SELECT 'vertical merge: kind before';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_vmerge' AND active AND column = 'lc';

OPTIMIZE TABLE t_auto_lc_vmerge FINAL;

SELECT 'vertical merge: kind after, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_vmerge' AND active AND column = 'lc';
SELECT count(), uniqExact(lc) FROM t_auto_lc_vmerge;

DROP TABLE t_auto_lc_vmerge;

-- 2) Encoded column as the sort key with unsorted input -> a permutation is required when writing.
DROP TABLE IF EXISTS t_auto_lc_orderby;
CREATE TABLE t_auto_lc_orderby
(
    lc String STATISTICS(uniq),
    id UInt64
)
ENGINE = MergeTree
ORDER BY lc
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

-- Lexicographically unsorted with respect to `lc`, so the writer needs a permutation.
INSERT INTO t_auto_lc_orderby SELECT 'k_' || toString(number % 20), number FROM numbers(5000);

SELECT 'order by encoded column: kind, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_orderby' AND active AND column = 'lc';
SELECT count(), uniqExact(lc), min(lc), max(lc) FROM t_auto_lc_orderby;

DROP TABLE t_auto_lc_orderby;

-- 3) ALTER that changes the type away from String: the LowCardinality kind must not be carried over.
DROP TABLE IF EXISTS t_auto_lc_alter;
CREATE TABLE t_auto_lc_alter
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 1000, min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc_alter SELECT number, toString(number % 10) FROM numbers(2000);

SELECT 'alter: kind before';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_alter' AND active AND column = 'lc';

ALTER TABLE t_auto_lc_alter MODIFY COLUMN lc UInt64;

SELECT 'alter: kind after (LowCardinality dropped, type is no longer String), correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_alter' AND active AND column = 'lc';
SELECT toTypeName(lc), count(), sum(lc) FROM t_auto_lc_alter;

DROP TABLE t_auto_lc_alter;

-- 4) Mutation that rewrites the encoded column (uses `MergedColumnOnlyOutputStream`).
DROP TABLE IF EXISTS t_auto_lc_mutate;
CREATE TABLE t_auto_lc_mutate
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 1000, min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc_mutate SELECT number, 'm_' || toString(number % 10) FROM numbers(2000);

ALTER TABLE t_auto_lc_mutate UPDATE lc = concat(lc, '!') WHERE id % 2 = 0;

SELECT 'mutation: kind after, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_mutate' AND active AND column = 'lc';
SELECT count(), uniqExact(lc), countIf(lc LIKE '%!') FROM t_auto_lc_mutate;

DROP TABLE t_auto_lc_mutate;
