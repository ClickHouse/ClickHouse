-- Regression test for `optimize_trivial_count_with_sparsity_filter`. The rewrite
-- serves `count() WHERE col <op> const` from per-column `num_defaults` /
-- `num_rows` in `serialization.json` when `<op> const` exactly partitions rows
-- into the column's defaults and non-defaults.

DROP TABLE IF EXISTS t_sparsity;

CREATE TABLE t_sparsity
(
    id UInt64,
    u_sparse UInt32,        -- ~95% defaults -> sparse-encoded
    u_dense Int32,          -- 50/50 -> default-encoded but sparse-eligible
    s_sparse String,        -- ~95% empty -> sparse-encoded
    s_dense String,         -- mostly non-empty -> default-encoded
    nul_sparse Nullable(Int32), -- ~95% NULL
    b_sparse Bool           -- ~95% false
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1,
    nullable_serialization_version = 'allow_sparse';

INSERT INTO t_sparsity
SELECT
    number,
    if (number % 20 = 0, number, 0),
    if (number % 2 = 0, 0, 100),
    if (number % 20 = 0, toString(number), ''),
    if (number % 100 = 0, '', concat('row', toString(number))),
    if (number % 20 = 0, number::Int32, NULL),
    number % 20 = 0
FROM numbers(10000);

-- Counts must match the baseline regardless of which encoding kind was chosen.
SELECT 'u_sparse = 0',          count() FROM t_sparsity WHERE u_sparse = 0    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'u_sparse = 0 baseline', count() FROM t_sparsity WHERE u_sparse = 0    SETTINGS optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'u_sparse > 0',          count() FROM t_sparsity WHERE u_sparse > 0    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'u_sparse >= 1',         count() FROM t_sparsity WHERE u_sparse >= 1   SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'u_sparse < 1',          count() FROM t_sparsity WHERE u_sparse < 1    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'u_sparse <= 0',         count() FROM t_sparsity WHERE u_sparse <= 0   SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'u_sparse != 0',         count() FROM t_sparsity WHERE u_sparse != 0   SETTINGS optimize_trivial_count_with_sparsity_filter = 1;

SELECT 'u_dense = 0',           count() FROM t_sparsity WHERE u_dense = 0     SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'u_dense = 0 baseline',  count() FROM t_sparsity WHERE u_dense = 0     SETTINGS optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'u_dense != 0',          count() FROM t_sparsity WHERE u_dense != 0    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
-- Asymmetric integer comparisons are restricted to unsigned columns; on an Int32
-- the rewrite must NOT fire (the partition isn't clean) but the answer must
-- still match the baseline.
SELECT 'u_dense > 0',           count() FROM t_sparsity WHERE u_dense > 0     SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'u_dense > 0 baseline',  count() FROM t_sparsity WHERE u_dense > 0     SETTINGS optimize_trivial_count_with_sparsity_filter = 0;

SELECT 'empty(s_sparse)',           count() FROM t_sparsity WHERE empty(s_sparse)    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'empty(s_sparse) baseline',  count() FROM t_sparsity WHERE empty(s_sparse)    SETTINGS optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'notEmpty(s_sparse)',        count() FROM t_sparsity WHERE notEmpty(s_sparse) SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'empty(s_dense)',            count() FROM t_sparsity WHERE empty(s_dense)     SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'empty(s_dense) baseline',   count() FROM t_sparsity WHERE empty(s_dense)     SETTINGS optimize_trivial_count_with_sparsity_filter = 0;

SELECT 'nul_sparse IS NULL',           count() FROM t_sparsity WHERE nul_sparse IS NULL     SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'nul_sparse IS NULL baseline',  count() FROM t_sparsity WHERE nul_sparse IS NULL     SETTINGS optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'nul_sparse IS NOT NULL',       count() FROM t_sparsity WHERE nul_sparse IS NOT NULL SETTINGS optimize_trivial_count_with_sparsity_filter = 1;

SELECT 'b_sparse = true',           count() FROM t_sparsity WHERE b_sparse = true  SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'b_sparse = true baseline',  count() FROM t_sparsity WHERE b_sparse = true  SETTINGS optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'b_sparse != true',          count() FROM t_sparsity WHERE b_sparse != true SETTINGS optimize_trivial_count_with_sparsity_filter = 1;

-- Predicate shapes that should NOT be recognised by the classifier. The answer
-- must still be correct (falls through to a normal scan).
SELECT 'unrecognised: u_sparse > 5',  count() FROM t_sparsity WHERE u_sparse > 5      SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'unrecognised: id > 0',        count() FROM t_sparsity WHERE id > 0            SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'unrecognised: AND',           count() FROM t_sparsity WHERE u_sparse = 0 AND u_dense = 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 1;

DROP TABLE t_sparsity;
