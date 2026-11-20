-- Setting `max_uniq_number_for_low_cardinality` back to 0 rolls the automatic `LowCardinality`
-- serialization back: a merge and a part rewrite drop the encoding inherited from the source parts.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;

-- 1) Merge.
DROP TABLE IF EXISTS t_auto_lc_rollback_merge;
CREATE TABLE t_auto_lc_rollback_merge
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

SYSTEM STOP MERGES t_auto_lc_rollback_merge;

INSERT INTO t_auto_lc_rollback_merge SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);
INSERT INTO t_auto_lc_rollback_merge SELECT number, 'w_' || toString(number % 8) FROM numbers(2000);

SELECT 'merge: kind with the feature enabled';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rollback_merge' AND active AND column = 'lc';

ALTER TABLE t_auto_lc_rollback_merge MODIFY SETTING max_uniq_number_for_low_cardinality = 0;
SYSTEM START MERGES t_auto_lc_rollback_merge;
OPTIMIZE TABLE t_auto_lc_rollback_merge FINAL;

SELECT 'merge: kind after the feature is disabled, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rollback_merge' AND active AND column = 'lc';
SELECT count(), uniqExact(lc) FROM t_auto_lc_rollback_merge;

DROP TABLE t_auto_lc_rollback_merge;

-- 2) Rewrite of the parts by a mutation.
DROP TABLE IF EXISTS t_auto_lc_rollback_rewrite;
CREATE TABLE t_auto_lc_rollback_rewrite
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

INSERT INTO t_auto_lc_rollback_rewrite SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'rewrite: kind with the feature enabled';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rollback_rewrite' AND active AND column = 'lc';

ALTER TABLE t_auto_lc_rollback_rewrite MODIFY SETTING max_uniq_number_for_low_cardinality = 0;
ALTER TABLE t_auto_lc_rollback_rewrite (REWRITE PARTS);

SELECT 'rewrite: kind after the feature is disabled, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rollback_rewrite' AND active AND column = 'lc';
SELECT count(), uniqExact(lc) FROM t_auto_lc_rollback_rewrite;

DROP TABLE t_auto_lc_rollback_rewrite;
