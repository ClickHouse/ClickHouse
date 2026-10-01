-- Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY: a vertical merge skips the snapshot-dead rows in every gathered column, applies a late kill, and indexes the right rows.
-- Red if a gathered column keeps the snapshot-dead rows: it shifts against `s` (4-row blocks, so the gathering spans blocks).
-- no-parallel: `unique_key_merge_pause_before_commit` is server-wide.

SET enable_unique_key = 1;

-- A run that stopped while the merge was paused leaves the fail point enabled.
SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit;

DROP TABLE IF EXISTS uk_vertical SYNC;

-- The UNIQUE KEY is not the ORDER BY prefix, so `id` and the other columns are all gathered.
CREATE TABLE uk_vertical (id UInt64, s UInt64, a String, b UInt32, c Float64, d Array(UInt8))
ENGINE = MergeTree ORDER BY s UNIQUE KEY (id)
SETTINGS merge_selector_algorithm = 'Manual', min_bytes_for_wide_part = 0,
         enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0,
         merge_max_block_size = 4, ratio_of_defaults_for_sparse_serialization = 1;

-- Two interleaved sources: `s` even in the first, odd in the second.
INSERT INTO uk_vertical SELECT number, number * 2, 'a' || toString(number), number, number / 2, [number] FROM numbers(10);
INSERT INTO uk_vertical SELECT 100 + number, number * 2 + 1, 'b' || toString(number), 100 + number, number / 4, [number, 1] FROM numbers(10);
-- Overwrites 2 and 5 in the first source and 103 in the second: dead at the merge's snapshot.
INSERT INTO uk_vertical VALUES (2, 100, 'new2', 2000, 0.5, [2]), (5, 101, 'new5', 5000, 0.5, [5]), (103, 102, 'new103', 1030, 0.5, [3]);

SYSTEM ENABLE FAILPOINT unique_key_merge_pause_before_commit;
SYSTEM SCHEDULE MERGE uk_vertical PARTS 'all_1_1_0', 'all_2_2_0', 'all_3_3_0';
SYSTEM WAIT FAILPOINT unique_key_merge_pause_before_commit PAUSE;

-- Late kill: red if a row killed during the merge stays live in the result.
DELETE FROM uk_vertical WHERE id IN (7, 106);

SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit;
SET max_execution_time = 120;
SYSTEM SYNC MERGES uk_vertical;
SET max_execution_time = 0;

SELECT 'row', _part_offset, id, s, a, b, c, d FROM uk_vertical ORDER BY s;

-- Merged index: red if an overwrite of a key that follows a snapshot-dead row kills another row.
INSERT INTO uk_vertical VALUES (6, 200, 'over6', 6000, 0.5, [6]), (104, 201, 'over104', 1040, 0.5, [4]);
SELECT 'overwrite', id, a FROM uk_vertical WHERE id IN (4, 6, 8, 102, 104, 105) ORDER BY id, a;
SELECT 'overwrite_count', count(), countDistinct(id) FROM uk_vertical; -- 18 18

-- The merge logs its part after the commit SYNC MERGES waits for; the drop waits for the whole merge task.
DROP TABLE uk_vertical SYNC;

-- Algorithm: red if a UNIQUE KEY merge stops choosing the vertical algorithm.
SYSTEM FLUSH LOGS part_log;
SELECT 'algorithm', merge_algorithm, rows FROM system.part_log
WHERE database = currentDatabase() AND table = 'uk_vertical' AND event_type = 'MergeParts';
