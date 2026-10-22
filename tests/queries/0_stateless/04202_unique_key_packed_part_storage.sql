-- Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY on Packed part storage, one case per writer.

SET enable_unique_key = 1;
SET async_insert = 0;
SET mutations_sync = 2;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit;

-- 1. INSERT overwrite: red if its bitmap cannot be staged into its Packed part.
DROP TABLE IF EXISTS uk_packed_insert;
CREATE TABLE uk_packed_insert (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
SYSTEM STOP MERGES uk_packed_insert;

INSERT INTO uk_packed_insert SELECT number, 'a' FROM numbers(10);
INSERT INTO uk_packed_insert VALUES (3, 'b'), (7, 'b');

SELECT 'insert', arraySort(groupArray((id, v))) FROM (SELECT id, v FROM uk_packed_insert WHERE id IN (2, 3, 7));
SELECT 'insert_storage', name, part_storage_type, unique_key_bitmap_versions FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_packed_insert' AND active ORDER BY name;
CHECK TABLE uk_packed_insert SETTINGS check_query_single_value_result = 1;

DROP TABLE uk_packed_insert;

-- 2. ignore: red if the bitmap killing the part's own rows cannot be staged.
DROP TABLE IF EXISTS uk_packed_ignore;
CREATE TABLE uk_packed_ignore (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS unique_key_conflict_action = 'ignore',
         min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
SYSTEM STOP MERGES uk_packed_ignore;

INSERT INTO uk_packed_ignore SELECT number, 'a' FROM numbers(5);
INSERT INTO uk_packed_ignore SELECT number, 'b' FROM numbers(3, 5);

SELECT 'ignore', arraySort(groupArray((id, v))) FROM (SELECT id, v FROM uk_packed_ignore);
SELECT 'ignore_storage', name, part_storage_type, rows, unique_key_bitmap_versions FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_packed_ignore' AND active ORDER BY name;

DROP TABLE uk_packed_ignore;

-- 3. dedup retry: red if the part left after dropping the replayed token stages into a sealed archive.
DROP TABLE IF EXISTS uk_packed_dedup;
CREATE TABLE uk_packed_dedup (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS non_replicated_deduplication_window = 100,
         min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
SYSTEM STOP MERGES uk_packed_dedup;

INSERT INTO uk_packed_dedup VALUES (2, 'old');

SET async_insert = 1, wait_for_async_insert = 0, async_insert_busy_timeout_min_ms = 600000,
    async_insert_busy_timeout_max_ms = 600000, async_insert_use_adaptive_busy_timeout = 0, insert_deduplicate = 1;

SET insert_deduplication_token = 'A';
INSERT INTO uk_packed_dedup VALUES (1, 'a');
SYSTEM FLUSH ASYNC INSERT QUEUE uk_packed_dedup;

INSERT INTO uk_packed_dedup VALUES (1, 'a');
SET insert_deduplication_token = 'B';
INSERT INTO uk_packed_dedup VALUES (2, 'b');
SYSTEM FLUSH ASYNC INSERT QUEUE uk_packed_dedup;

SET async_insert = 0, wait_for_async_insert = 1, insert_deduplication_token = '';

SELECT 'dedup', arraySort(groupArray((id, v))) FROM (SELECT id, v FROM uk_packed_dedup);
SELECT 'dedup_storage', groupUniqArray(part_storage_type) FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_packed_dedup' AND active;

DROP TABLE uk_packed_dedup;

-- 4. DELETE: red if the DELETE's 0-row marker cannot hold the bitmap it stages.
DROP TABLE IF EXISTS uk_packed_delete;
CREATE TABLE uk_packed_delete (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
SYSTEM STOP MERGES uk_packed_delete;

INSERT INTO uk_packed_delete SELECT number, 'a' FROM numbers(10);
DELETE FROM uk_packed_delete WHERE id IN (2, 5);

SELECT 'delete', arraySort(groupArray(id)) FROM uk_packed_delete;
SELECT 'delete_marker', part_storage_type, unique_key_bitmap_versions FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_packed_delete' AND active AND rows = 0;

DROP TABLE uk_packed_delete;

-- 5. merges: each case reads after an overwrite, since SYNC MERGES can return before the merge is visible.

-- 5a. late kill: red if the merged Packed part cannot hold a kill that lands while the merge runs.
DROP TABLE IF EXISTS uk_packed_merge;
CREATE TABLE uk_packed_merge (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS merge_selector_algorithm = 'Manual',
         min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;

INSERT INTO uk_packed_merge SELECT number, 'a' FROM numbers(10);
INSERT INTO uk_packed_merge SELECT number, 'b' FROM numbers(10, 10);

SYSTEM ENABLE FAILPOINT unique_key_merge_pause_before_commit;
SYSTEM SCHEDULE MERGE uk_packed_merge PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM WAIT FAILPOINT unique_key_merge_pause_before_commit PAUSE;

DELETE FROM uk_packed_merge WHERE id IN (3, 13);

SYSTEM DISABLE FAILPOINT unique_key_merge_pause_before_commit;
SET max_execution_time = 120;
SYSTEM SYNC MERGES uk_packed_merge;
SET max_execution_time = 0;

INSERT INTO uk_packed_merge VALUES (5, 'new'), (15, 'new');

SELECT 'late_kill', count(), countDistinct(id), countIf(id IN (3, 13)) FROM uk_packed_merge;
SELECT 'late_kill_probe', arraySort(groupArray((id, v))) FROM (SELECT id, v FROM uk_packed_merge WHERE id IN (4, 5, 6, 15));
SELECT 'late_kill_bitmaps', unique_key_bitmap_versions FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_packed_merge' AND active AND name = 'all_1_2_1';

DROP TABLE uk_packed_merge SYNC;

-- 5b. carry: red if merging the DELETE marker without its target fails.
DROP TABLE IF EXISTS uk_packed_carry;
CREATE TABLE uk_packed_carry (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS merge_selector_algorithm = 'Manual',
         min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;

INSERT INTO uk_packed_carry SELECT number, 'a' FROM numbers(10);
INSERT INTO uk_packed_carry SELECT number, 'b' FROM numbers(100, 10);
DELETE FROM uk_packed_carry WHERE id < 5;

SET max_execution_time = 120;
SYSTEM SCHEDULE MERGE uk_packed_carry PARTS 'all_2_2_0', 'all_3_3_0';
SYSTEM SYNC MERGES uk_packed_carry;
SET max_execution_time = 0;

INSERT INTO uk_packed_carry VALUES (100, 'new');

SELECT 'carry_rows', count(), min(id), anyIf(v, id = 100) FROM uk_packed_carry;
SELECT 'carry', name, arrayMap(x -> replaceRegexpOne(x, '^[0-9]+_for_', 'csn_for_'), unique_key_bitmap_versions)
FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_packed_carry' AND active AND name IN ('all_1_1_0', 'all_2_3_1')
ORDER BY name;

DROP TABLE uk_packed_carry SYNC;

SYSTEM FLUSH LOGS part_log;
SELECT 'merged_storage', table, part_name, part_storage_type FROM system.part_log
WHERE database = currentDatabase() AND table IN ('uk_packed_merge', 'uk_packed_carry') AND event_type = 'MergeParts'
ORDER BY table;

-- 6. mutations: red if the mutation fails; the UPDATE rewrites one part and clones two.
DROP TABLE IF EXISTS uk_packed_mutation;
CREATE TABLE uk_packed_mutation (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS merge_selector_algorithm = 'Manual',
         min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;

INSERT INTO uk_packed_mutation SELECT number, 'a' FROM numbers(10);
INSERT INTO uk_packed_mutation SELECT number, 'a' FROM numbers(10, 10);
INSERT INTO uk_packed_mutation VALUES (3, 'x'), (13, 'x');

ALTER TABLE uk_packed_mutation UPDATE v = 'u' WHERE id = 5;

SELECT 'mutation', arraySort(groupArray((id, v))) FROM (SELECT id, v FROM uk_packed_mutation WHERE id IN (3, 5, 13));
SELECT 'mutation_count', count(), countDistinct(id) FROM uk_packed_mutation;
SELECT 'mutation_storage', groupUniqArray(part_storage_type), count() FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_packed_mutation' AND active AND data_version > min_block_number;

INSERT INTO uk_packed_mutation VALUES (3, 'y'), (5, 'y'), (13, 'y');
SELECT 'mutation_probe', arraySort(groupArray((id, v))) FROM (SELECT id, v FROM uk_packed_mutation WHERE id IN (3, 5, 13));
SELECT 'mutation_probe_count', count(), countDistinct(id) FROM uk_packed_mutation;

DROP TABLE uk_packed_mutation;

-- 7. DETACH / ATTACH: red if a reattached Packed part loses its bitmap or its dense index.
DROP TABLE IF EXISTS uk_packed_attach;
CREATE TABLE uk_packed_attach (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
SYSTEM STOP MERGES uk_packed_attach;

INSERT INTO uk_packed_attach SELECT number, 'a' FROM numbers(10);
INSERT INTO uk_packed_attach VALUES (3, 'b'), (7, 'b');

DETACH TABLE uk_packed_attach;
ATTACH TABLE uk_packed_attach;
SYSTEM STOP MERGES uk_packed_attach;

SELECT 'reattach', arraySort(groupArray((id, v))) FROM (SELECT id, v FROM uk_packed_attach WHERE id IN (2, 3, 7));
SELECT 'reattach_storage', groupUniqArray(part_storage_type) FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_packed_attach' AND active;
INSERT INTO uk_packed_attach VALUES (3, 'c');
SELECT 'reattach_probe', arraySort(groupArray((id, v))) FROM (SELECT id, v FROM uk_packed_attach WHERE id IN (2, 3, 7));
SELECT 'reattach_probe_count', count(), countDistinct(id) FROM uk_packed_attach;

DROP TABLE uk_packed_attach;
