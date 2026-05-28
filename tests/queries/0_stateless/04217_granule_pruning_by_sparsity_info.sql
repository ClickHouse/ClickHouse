-- Granule-level pruning for sparse-encoded columns in `planning` mode trims
-- `MarkRanges` per part from the predicate column's offsets stream.

DROP TABLE IF EXISTS t_granule_prune;

CREATE TABLE t_granule_prune
(
    id UInt64,
    x UInt32
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, ratio_of_defaults_for_sparse_serialization = 0.5;

SYSTEM STOP MERGES t_granule_prune;

-- 1000 rows split into 10 granules of 100 rows. Non-defaults only at rows 0, 200,
-- 400, 600, 800 -> non-defaults land in granules 0, 2, 4, 6, 8. Granules 1, 3, 5,
-- 7, 9 are all-default and prunable for `WHERE x != 0`; conversely no granule is
-- all-non-default, so `WHERE x = 0` cannot be pruned.
INSERT INTO t_granule_prune SELECT number, if(number % 200 = 0, 1, 0) FROM numbers(1000) SETTINGS optimize_on_insert = 0;

SELECT serialization_kind FROM system.parts_columns
WHERE table = 't_granule_prune' AND database = currentDatabase() AND column = 'x';

-- Ground truth.
SELECT 'sumIf', sumIf(x, x != 0) FROM t_granule_prune;
SELECT 'countIf', countIf(x != 0) FROM t_granule_prune;

-- `planning` mode: `count()` and `sum(x)` must equal the baseline (5 rows of `x = 1`).
SELECT 'planning count', count() FROM t_granule_prune WHERE x != 0 SETTINGS
    optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'planning';
SELECT 'planning sum',   sum(x)  FROM t_granule_prune WHERE x != 0 SETTINGS
    optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'planning';

-- `off` mode (baseline), must match.
SELECT 'off count', count() FROM t_granule_prune WHERE x != 0 SETTINGS
    optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'off';
SELECT 'off sum',   sum(x)  FROM t_granule_prune WHERE x != 0 SETTINGS
    optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'off';

-- `WHERE x = 0` cannot be pruned: no granule is all-non-default, so nothing drops.
SELECT 'unprunable planning', count() FROM t_granule_prune WHERE x = 0 SETTINGS
    optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'planning';
SELECT 'unprunable off',      count() FROM t_granule_prune WHERE x = 0 SETTINGS
    optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'off';

-- EXPLAIN: the `Sparsity` step drops 5 of 10 granules for `WHERE x != 0`.
-- Strip the surrounding plan so the assertion is robust to clickhouse-test random
-- settings that rename steps.
SELECT explain FROM (
  EXPLAIN indexes = 1 SELECT id FROM t_granule_prune WHERE x != 0
  SETTINGS use_sparsity_info_for_pruning = 'planning'
) WHERE trimLeft(explain) LIKE 'Sparsity%'
   OR trimLeft(explain) LIKE 'Parts: %'
   OR trimLeft(explain) LIKE 'Granules: %'
   OR trimLeft(explain) LIKE 'x';

DROP TABLE t_granule_prune;
