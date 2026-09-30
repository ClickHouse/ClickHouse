-- Tags: no-fasttest, no-ordinary-database, no-async-insert, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY insert shapes under `overwrite`: the probe finds and kills every earlier live copy of a key.
--   1. partitions: the same key in two partitions coexists
--   2. oldest part: one INSERT kills rows in three of 10 parts, the oldest included
--   3. one INSERT, many parts: keys repeating across its own blocks keep the last value
-- no-async-insert: one part per INSERT is asserted. `ignore` is 04168's, `abort` 04174's.

SET enable_unique_key = 1;

SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS uk_dedup_part;
DROP TABLE IF EXISTS uk_many_parts;
DROP TABLE IF EXISTS uk_mp_ow;

-- 1. partitions: red if the INSERT probes every partition for its keys (`1 42 part1` goes).
CREATE TABLE uk_dedup_part (part_key UInt32, id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id) PARTITION BY part_key;
SYSTEM STOP MERGES uk_dedup_part;

INSERT INTO uk_dedup_part VALUES (1, 42, 'part1');
INSERT INTO uk_dedup_part VALUES (2, 42, 'part2');
SELECT part_key, id, v FROM uk_dedup_part ORDER BY part_key;

INSERT INTO uk_dedup_part VALUES (1, 42, 'part1_new');
SELECT part_key, v FROM uk_dedup_part WHERE id = 42 ORDER BY part_key;

-- 2. oldest part: red if the probe leaves the oldest part out (`oldest_hits` 2, `oldest_rows` 11), or
-- conflict resolution sees no live hit (`oldest_hits` 0, `oldest_rows` 13).
CREATE TABLE uk_many_parts (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         parts_to_delay_insert = 10000, parts_to_throw_insert = 20000,
         max_bytes_to_merge_at_max_space_in_pool = 1;

SYSTEM STOP MERGES uk_many_parts;

INSERT INTO uk_many_parts SELECT 0 AS id, 'oldest' AS v;
INSERT INTO uk_many_parts SELECT 1 AS id, 'v_1' AS v;
INSERT INTO uk_many_parts SELECT 2 AS id, 'v_2' AS v;
INSERT INTO uk_many_parts SELECT 3 AS id, 'v_3' AS v;
INSERT INTO uk_many_parts SELECT 4 AS id, 'v_4' AS v;
INSERT INTO uk_many_parts SELECT 5 AS id, 'v_5' AS v;
INSERT INTO uk_many_parts SELECT 6 AS id, 'v_6' AS v;
INSERT INTO uk_many_parts SELECT 7 AS id, 'v_7' AS v;
INSERT INTO uk_many_parts SELECT 8 AS id, 'v_8' AS v;
INSERT INTO uk_many_parts SELECT 9 AS id, 'v_9' AS v;

INSERT INTO uk_many_parts SELECT * FROM values((0, 'new_oldest'), (4, 'new_4'), (9, 'new_9')) SETTINGS log_queries = 1;
SYSTEM FLUSH LOGS query_log;

SELECT 'oldest_hits', ProfileEvents['UniqueKeyConflictOverwriteRows'] FROM system.query_log
WHERE event_date >= yesterday()
  AND current_database = currentDatabase()
  AND query_kind = 'Insert'
  AND query LIKE '%uk_many_parts%new_oldest%'
  AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

SELECT 'oldest_rows', count() FROM uk_many_parts;  -- 10
SELECT 'oldest_overwritten', id, v FROM uk_many_parts WHERE v LIKE 'new_%' ORDER BY id;  -- 0 new_oldest / 4 new_4 / 9 new_9

-- 3. one INSERT, many parts: red if the probe reads the parts at the block's transaction
-- snapshot, missing the earlier blocks of its own INSERT (a debug server aborts on two live copies).
-- 2400 rows in 400-row blocks, one thread; `many_parts_made` proves several parts were made.
SET max_threads = 1;

CREATE TABLE uk_mp_ow (id UInt32, v UInt64)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES uk_mp_ow;

INSERT INTO uk_mp_ow
SELECT number % 600 AS id, number AS v FROM numbers(2400)
SETTINGS max_block_size = 400, max_insert_block_size = 400,
         min_insert_block_size_rows = 400, min_insert_block_size_bytes = 0;

SELECT 'many_parts_made', count() > 1 FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_mp_ow' AND active;  -- 1
SELECT 'many_parts_rows', count() FROM uk_mp_ow;  -- 600
-- The highest number with number % 600 = 5 in [0, 2400) is 1805.
SELECT 'many_parts_last_value', v FROM uk_mp_ow WHERE id = 5;  -- 1805

SET max_threads = DEFAULT;

DROP TABLE uk_dedup_part;
DROP TABLE uk_many_parts;
DROP TABLE uk_mp_ow;
