-- Sparsity based count and pruning act only when `serialization.json` carries an
-- `exact_num_defaults: true` flag for the column. The flag must survive a metadata
-- reload, and the merged stats of `OPTIMIZE FINAL` must stay exact when every input
-- part is exact.

DROP TABLE IF EXISTS t_persist;

CREATE TABLE t_persist
(
    id UInt64,
    x UInt32,
    n Nullable(UInt32)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512,
         ratio_of_defaults_for_sparse_serialization = 0.5,
         nullable_serialization_version = 'allow_sparse',
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_persist;

INSERT INTO t_persist
SELECT number,
       if(number < 3000, 0, 1)::UInt32,
       if(number < 3000, NULL, toUInt32(0))
FROM numbers(5000) SETTINGS optimize_on_insert = 0;

INSERT INTO t_persist
SELECT number + 5000,
       if(number < 3000, 0, 1)::UInt32,
       if(number < 3000, NULL, toUInt32(0))
FROM numbers(5000) SETTINGS optimize_on_insert = 0;

SELECT 'parts:', count() FROM system.parts
WHERE table = 't_persist' AND database = currentDatabase() AND active;

-- 1. Freshly written parts.
SELECT 'fresh x!=0',    trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE x != 0      SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';
SELECT 'fresh isNull',  trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE n IS NULL    SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';
SELECT 'fresh isNot',   trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE n IS NOT NULL SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';

-- 2. After DETACH and ATTACH parts are reloaded from disk. The optimization can only
-- still fire if the flag came back from `serialization.json`.
DETACH TABLE t_persist;
ATTACH TABLE t_persist;

SELECT 'reload x!=0',   trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE x != 0      SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';
SELECT 'reload isNull', trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE n IS NULL    SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';
SELECT 'reload isNot',  trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE n IS NOT NULL SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';

-- 3. OPTIMIZE FINAL merges the two parts. The merged stats must keep the flag set
-- when every input part has it set.
SYSTEM START MERGES t_persist;
OPTIMIZE TABLE t_persist FINAL;

SELECT 'parts after merge:', count() FROM system.parts
WHERE table = 't_persist' AND database = currentDatabase() AND active;

SELECT 'merged x!=0',   trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE x != 0      SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';
SELECT 'merged isNull', trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE n IS NULL    SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';
SELECT 'merged isNot',  trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_persist WHERE n IS NOT NULL SETTINGS optimize_trivial_count_with_sparsity_filter = 1) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';

-- 4. Granule level pruning (`Sparsity` step) on the merged part.
SELECT 'planning x!=0',   trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_persist WHERE x != 0      SETTINGS use_sparsity_info_for_pruning = 'planning', optimize_trivial_count_with_sparsity_filter = 0) WHERE trimLeft(explain) LIKE 'Sparsity%';
SELECT 'planning isNull', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_persist WHERE n IS NULL    SETTINGS use_sparsity_info_for_pruning = 'planning', optimize_trivial_count_with_sparsity_filter = 0) WHERE trimLeft(explain) LIKE 'Sparsity%';
SELECT 'planning isNot',  trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_persist WHERE n IS NOT NULL SETTINGS use_sparsity_info_for_pruning = 'planning', optimize_trivial_count_with_sparsity_filter = 0) WHERE trimLeft(explain) LIKE 'Sparsity%';

DROP TABLE t_persist;

-- 5. A Nullable column `m` coexists with a literal column called `m.null`. In that
-- case `WHERE m.null` refers to the literal column and must not be answered from
-- `m`'s NULL count.
DROP TABLE IF EXISTS t_null_conflict;
CREATE TABLE t_null_conflict (id UInt64, m Nullable(UInt32), `m.null` UInt8)
ENGINE = MergeTree ORDER BY id;
-- Pick the data so `count(m IS NULL) = 2` but `count(m.null = 1) = 3`. If the
-- classifier wrongly treated `WHERE m.null` as `m IS NULL`, the trivial count rewrite
-- would emit 2 instead of 3.
INSERT INTO t_null_conflict VALUES (1, 5, 0), (2, NULL, 1), (3, NULL, 1), (4, 7, 1);

SELECT 'conflict m IS NULL value:'  , countIf(m IS NULL)                 FROM t_null_conflict;
SELECT 'conflict m.null value:'     , countIf(`m.null` = 1)              FROM t_null_conflict;
SELECT 'conflict trivial m.null:'   , count() FROM t_null_conflict WHERE `m.null`
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;

-- `WHERE m.null` must not produce an `Optimized trivial count...` step in the plan.
-- The empty selection here means the rewrite did not fire.
SELECT 'conflict plan m.null:', trimLeft(explain) FROM (
    EXPLAIN SELECT count() FROM t_null_conflict WHERE `m.null`
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1
) WHERE explain LIKE '%Optimized trivial count with sparsity filter%';

DROP TABLE t_null_conflict;
