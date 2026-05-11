-- Regression test for `use_sparsity_info_for_pruning`. The optimisation
-- drops whole parts when the per-part `num_defaults` / `num_rows` recorded in
-- `serialization.json` prove the WHERE predicate is identically false on every row.
-- Two extremal shapes:
--   1. `col != default(col)` AND part has `num_defaults == num_rows` (all default).
--   2. `col =  default(col)` AND part has `num_defaults == 0`           (no default).
-- Parts that fall in between -- a non-zero non-`num_rows` `num_defaults` -- must
-- never be pruned, no matter how few or many non-defaults they hold.

DROP TABLE IF EXISTS t_sparsity_prune;

CREATE TABLE t_sparsity_prune
(
    id UInt64,
    all_default UInt32,
    no_default UInt32,
    mixed UInt32
)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, index_granularity = 8192;

SYSTEM STOP MERGES t_sparsity_prune;

-- Three parts with deliberately different default ratios per column. Pruning must
-- act on every part independently (drop only the extremal ones).
INSERT INTO t_sparsity_prune SELECT number,        0,   100, if(number % 2 = 0, 0, 100) FROM numbers(1000); -- p1: all_default all-default, no_default all non-default, mixed 50/50
INSERT INTO t_sparsity_prune SELECT number+1000,   0,   100, 0                          FROM numbers(1000); -- p2: all_default all-default, no_default all non-default, mixed all-default
INSERT INTO t_sparsity_prune SELECT number+2000, 100,     0, 50                         FROM numbers(1000); -- p3: all_default all non-default, no_default all-default, mixed all-non-default
-- p4: a single non-default row in `all_default` -- the column-wide "all default" no longer holds, so the part must NOT be pruned.
INSERT INTO t_sparsity_prune SELECT number+3000, if(number = 0, 7, 0), 100, 0 FROM numbers(1000);

-- The test would be unsafe to assert on if the writer hadn't marked counts as exact.
SELECT name, column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparsity_prune' AND database = currentDatabase()
ORDER BY name, column;

-- Aggregate counts must equal baseline regardless of which parts get pruned.
SELECT 'all_default != 0',          count() FROM t_sparsity_prune WHERE all_default != 0  SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'all_default != 0 baseline', count() FROM t_sparsity_prune WHERE all_default != 0  SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT 'all_default  = 0',          count() FROM t_sparsity_prune WHERE all_default  = 0  SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'all_default  = 0 baseline', count() FROM t_sparsity_prune WHERE all_default  = 0  SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT 'no_default  = 0',           count() FROM t_sparsity_prune WHERE no_default  = 0   SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'no_default  = 0 baseline',  count() FROM t_sparsity_prune WHERE no_default  = 0   SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT 'no_default != 0',           count() FROM t_sparsity_prune WHERE no_default != 0   SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'no_default != 0 baseline',  count() FROM t_sparsity_prune WHERE no_default != 0   SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT 'mixed != 0',                count() FROM t_sparsity_prune WHERE mixed != 0        SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'mixed != 0 baseline',       count() FROM t_sparsity_prune WHERE mixed != 0        SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT 'mixed  = 0',                count() FROM t_sparsity_prune WHERE mixed  = 0        SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'mixed  = 0 baseline',       count() FROM t_sparsity_prune WHERE mixed  = 0        SETTINGS use_sparsity_info_for_pruning = 'off';

-- Non-count projection benefits too (Layer C-1 trivial-count wouldn't apply here).
SELECT 'projection drop-extremal',          count() FROM (SELECT id FROM t_sparsity_prune WHERE all_default != 0 SETTINGS use_sparsity_info_for_pruning = 'planning');
SELECT 'projection drop-extremal baseline', count() FROM (SELECT id FROM t_sparsity_prune WHERE all_default != 0 SETTINGS use_sparsity_info_for_pruning = 'off');

-- EXPLAIN: Sparsity step prunes only the parts that satisfy the extremal condition.
-- We strip out the surrounding plan lines so the assertion is robust to plan-shape
-- differences that random clickhouse-test settings can introduce (Expression vs Filter
-- step naming, extra rename steps, etc.).
-- `all_default != 0` -- p1 and p2 are all-default, dropped; p3 and p4 are kept.
SELECT explain FROM (
  EXPLAIN indexes = 1 SELECT id FROM t_sparsity_prune WHERE all_default != 0
  SETTINGS use_sparsity_info_for_pruning = 'planning'
) WHERE trimLeft(explain) LIKE 'Sparsity%' OR trimLeft(explain) LIKE 'Parts: %' OR trimLeft(explain) LIKE 'Granules: %' OR trimLeft(explain) LIKE 'all_default';

-- `no_default = 0` -- only p3 has any defaults in no_default, the other three parts are dropped.
SELECT explain FROM (
  EXPLAIN indexes = 1 SELECT id FROM t_sparsity_prune WHERE no_default = 0
  SETTINGS use_sparsity_info_for_pruning = 'planning'
) WHERE trimLeft(explain) LIKE 'Sparsity%' OR trimLeft(explain) LIKE 'Parts: %' OR trimLeft(explain) LIKE 'Granules: %' OR trimLeft(explain) LIKE 'no_default';

-- `mixed != 0` -- p2 and p4 are all-default in `mixed`, dropped; p1 and p3 kept.
SELECT explain FROM (
  EXPLAIN indexes = 1 SELECT id FROM t_sparsity_prune WHERE mixed != 0
  SETTINGS use_sparsity_info_for_pruning = 'planning'
) WHERE trimLeft(explain) LIKE 'Sparsity%' OR trimLeft(explain) LIKE 'Parts: %' OR trimLeft(explain) LIKE 'Granules: %' OR trimLeft(explain) LIKE 'mixed';

DROP TABLE t_sparsity_prune;
