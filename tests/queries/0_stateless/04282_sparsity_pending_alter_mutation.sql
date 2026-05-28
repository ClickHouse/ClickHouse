-- Tags: no-parallel-replicas

-- A pending `MODIFY COLUMN` leaves the on-disk `num_defaults` in source-type units
-- while reads convert to the new type. Sparsity-based pruning and the trivial-count
-- rewrite must opt out under any pending alter mutation: otherwise predicates whose
-- default/non-default partition changes across the conversion return wrong counts.

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS t_alter_sparse_pending;

CREATE TABLE t_alter_sparse_pending (id UInt64, n UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_alter_sparse_pending;

-- Two parts:
--   part 1 has 5000 UInt32 zeros (num_defaults = 5000, sparse)
--   part 2 has 5000 UInt32 ones  (num_defaults = 0,    dense)
INSERT INTO t_alter_sparse_pending SELECT number, 0::UInt32 FROM numbers(5000)
    SETTINGS optimize_on_insert = 0;
INSERT INTO t_alter_sparse_pending SELECT number + 5000, 1::UInt32 FROM numbers(5000)
    SETTINGS optimize_on_insert = 0;

-- Convert to Nullable(UInt32). On-disk data stays UInt32; reads present them as
-- non-NULL nullables. The stored `num_defaults` (counted as UInt32 zeros) is now
-- stale because the new type's default is NULL, not 0.
ALTER TABLE t_alter_sparse_pending MODIFY COLUMN n Nullable(UInt32) DEFAULT NULL
    SETTINGS mutations_sync = 0, alter_sync = 0;

-- Truths without any sparsity machinery.
SELECT 'truth_isnull',    countIf(n IS NULL)     FROM t_alter_sparse_pending
    SETTINGS use_sparsity_info_for_pruning = 'off', optimize_trivial_count_with_sparsity_filter = 0,
             optimize_functions_to_subcolumns = 0;
SELECT 'truth_isnotnull', countIf(n IS NOT NULL) FROM t_alter_sparse_pending
    SETTINGS use_sparsity_info_for_pruning = 'off', optimize_trivial_count_with_sparsity_filter = 0,
             optimize_functions_to_subcolumns = 0;

-- Trivial-count rewrite. Bug returns num_defaults (UInt32 zeros) instead of NULL count.
SELECT 'rewrite_isnull',    count() FROM t_alter_sparse_pending WHERE n IS NULL
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1,
             optimize_functions_to_subcolumns = 0;
SELECT 'rewrite_isnotnull', count() FROM t_alter_sparse_pending WHERE n IS NOT NULL
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1,
             optimize_functions_to_subcolumns = 0;

-- Planning-mode part pruning. For `IS NOT NULL` (MatchesNonDefault) bug drops part 1
-- because the old stat says all 5000 rows are "defaults", returning 5000 instead of 10000.
-- For `IS NULL` bug drops part 2 because its old stat is num_defaults = 0; the
-- surviving part 1 has 0 NULLs after conversion so the visible count happens to match
-- truth, but the prune was still unsafe and on different data would diverge.
SELECT 'planning_isnull',    count() FROM t_alter_sparse_pending WHERE n IS NULL
    SETTINGS use_sparsity_info_for_pruning = 'planning', optimize_trivial_count_with_sparsity_filter = 0,
             optimize_functions_to_subcolumns = 0;
SELECT 'planning_isnotnull', count() FROM t_alter_sparse_pending WHERE n IS NOT NULL
    SETTINGS use_sparsity_info_for_pruning = 'planning', optimize_trivial_count_with_sparsity_filter = 0,
             optimize_functions_to_subcolumns = 0;

-- Data-read-mode lazy classification. Same hazard at scan time.
SELECT 'data_read_isnull',    count() FROM t_alter_sparse_pending WHERE n IS NULL
    SETTINGS use_sparsity_info_for_pruning = 'data_read', optimize_trivial_count_with_sparsity_filter = 0,
             optimize_functions_to_subcolumns = 0;
SELECT 'data_read_isnotnull', count() FROM t_alter_sparse_pending WHERE n IS NOT NULL
    SETTINGS use_sparsity_info_for_pruning = 'data_read', optimize_trivial_count_with_sparsity_filter = 0,
             optimize_functions_to_subcolumns = 0;

SYSTEM START MERGES t_alter_sparse_pending;
DROP TABLE t_alter_sparse_pending;
