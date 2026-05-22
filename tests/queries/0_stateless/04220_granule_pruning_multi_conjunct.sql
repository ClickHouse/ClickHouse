-- `collectSparsityConjuncts` flattens top-level `AND` and feeds every classified
-- conjunct to the pruning step. A part / granule is dropped when any single
-- conjunct rules it out. The trivial-count rewrite remains single-predicate only.

DROP TABLE IF EXISTS t_multi_conjunct;

CREATE TABLE t_multi_conjunct
(
    id UInt64,
    a UInt32,
    b UInt32
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, ratio_of_defaults_for_sparse_serialization = 0.5;

SYSTEM STOP MERGES t_multi_conjunct;

-- `a != 0` at rows 200, 400, 600, 800 (granules 2, 4, 6, 8).
-- `b != 0` at rows 300, 600, 900           (granules 3, 6, 9).
-- WHERE a != 0 AND b != 0 matches only row 600 (granule 6). Granule pruning should
-- drop the other 9 granules: each has at least one column that is all-default.
INSERT INTO t_multi_conjunct
SELECT number,
       if(number % 200 = 0 AND number > 0, number, 0),
       if(number % 300 = 0 AND number > 0, number, 0)
FROM numbers(1000) SETTINGS optimize_on_insert = 0;

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_multi_conjunct' AND database = currentDatabase() AND column IN ('a', 'b')
ORDER BY column;

SET optimize_trivial_count_with_sparsity_filter = 0;

SELECT 'baseline countIf', countIf(a != 0 AND b != 0) FROM t_multi_conjunct;
SELECT 'baseline sum',     sumIf(a + b, a != 0 AND b != 0) FROM t_multi_conjunct;

-- All three modes must agree with baseline.
SELECT 'off',       count() FROM t_multi_conjunct WHERE a != 0 AND b != 0 SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT 'planning',  count() FROM t_multi_conjunct WHERE a != 0 AND b != 0 SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'data_read', count() FROM t_multi_conjunct WHERE a != 0 AND b != 0 SETTINGS use_sparsity_info_for_pruning = 'data_read';

SELECT 'off sum',       sum(a + b) FROM t_multi_conjunct WHERE a != 0 AND b != 0 SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT 'planning sum',  sum(a + b) FROM t_multi_conjunct WHERE a != 0 AND b != 0 SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'data_read sum', sum(a + b) FROM t_multi_conjunct WHERE a != 0 AND b != 0 SETTINGS use_sparsity_info_for_pruning = 'data_read';

-- EXPLAIN: `Sparsity` step prunes 9 of 10 granules and lists both `a` and `b` as keys.
SELECT explain FROM (
  EXPLAIN indexes = 1 SELECT id FROM t_multi_conjunct WHERE a != 0 AND b != 0
  SETTINGS use_sparsity_info_for_pruning = 'planning'
) WHERE trimLeft(explain) LIKE 'Sparsity%'
   OR trimLeft(explain) LIKE 'Parts: %'
   OR trimLeft(explain) LIKE 'Granules: %'
   OR trimLeft(explain) IN ('a', 'b');

-- A conjunct that doesn't classify (e.g. `id < 1000`) doesn't disable the others.
-- The classified conjunct still drives the granule prune; the unclassified one is
-- just evaluated normally.
SELECT 'mixed planning',  count() FROM t_multi_conjunct WHERE a != 0 AND id < 1000 SETTINGS use_sparsity_info_for_pruning = 'planning';
SELECT 'mixed data_read', count() FROM t_multi_conjunct WHERE a != 0 AND id < 1000 SETTINGS use_sparsity_info_for_pruning = 'data_read';
SELECT 'mixed off',       count() FROM t_multi_conjunct WHERE a != 0 AND id < 1000 SETTINGS use_sparsity_info_for_pruning = 'off';

DROP TABLE t_multi_conjunct;
