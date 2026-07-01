-- The row/default counts persisted in `serialization.json` must survive the write paths that do not
-- rewrite all columns: vertical merges (gathered columns are written through separate column-only
-- streams) and column-only mutations (untouched columns are hardlinked from the source part). If the
-- counts are lost, the next merge sees zero counts and silently falls back from sparse to full
-- serialization. `DETACH`/`ATTACH` in between forces the counts to be re-read from `serialization.json`,
-- so the file itself is checked, not only the in-memory part.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_sparse_counts;

CREATE TABLE t_sparse_counts (id UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.5,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_sparse_counts;
INSERT INTO t_sparse_counts SELECT number, number, if(number % 100 = 0, 'x', '') FROM numbers(50000);
INSERT INTO t_sparse_counts SELECT number, number, if(number % 100 = 0, 'x', '') FROM numbers(50000);
SYSTEM START MERGES t_sparse_counts;

-- (1) A vertical merge writes `s` through a column-only stream.
OPTIMIZE TABLE t_sparse_counts FINAL;

SYSTEM FLUSH LOGS part_log;
SELECT 'merge_algorithm', merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_sparse_counts' AND event_type = 'MergeParts' AND part_name = 'all_1_2_1';

SELECT 'after_vertical_merge', serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_counts' AND column = 's' AND active;

-- (2) The next merge re-decides the kind from the counts the vertical merge persisted.
DETACH TABLE t_sparse_counts;
ATTACH TABLE t_sparse_counts;

OPTIMIZE TABLE t_sparse_counts FINAL;
SELECT 'after_second_merge', serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_counts' AND column = 's' AND active;

-- (3) A column-only mutation rewrites `v` and hardlinks `s`; the counts of `s` must be carried over.
ALTER TABLE t_sparse_counts UPDATE v = v + 1 WHERE 1;

DETACH TABLE t_sparse_counts;
ATTACH TABLE t_sparse_counts;

OPTIMIZE TABLE t_sparse_counts FINAL;
SELECT 'after_update_and_merge', serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_counts' AND column = 's' AND active;

-- (4) A mutation that rewrites no columns at all (pure hardlink) must carry the counts over too.
ALTER TABLE t_sparse_counts DROP COLUMN v;

DETACH TABLE t_sparse_counts;
ATTACH TABLE t_sparse_counts;

OPTIMIZE TABLE t_sparse_counts FINAL;
SELECT 'after_drop_column_and_merge', serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_counts' AND column = 's' AND active;

-- The data is intact.
SELECT 'count', count() FROM t_sparse_counts;
SELECT 'nonempty', count() FROM t_sparse_counts WHERE s != '';

DROP TABLE t_sparse_counts;
