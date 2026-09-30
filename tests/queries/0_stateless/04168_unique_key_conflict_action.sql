-- Tags: no-fasttest, no-ordinary-database, no-async-insert, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY `unique_key_conflict_action = 'ignore'`, and repointing the policy. Overwrite is 04105's, abort 04174's.
--   1. ignore, mixed batch: only new keys land
--   1b. ignore on a partitioned table: the rewritten part keeps its partition

SET enable_unique_key = 1;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS uk_ig_mix;
DROP TABLE IF EXISTS uk_ig_part;

-- 1. ignore, mixed batch: red if the rewritten part keeps a conflicting row.
CREATE TABLE uk_ig_mix (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, unique_key_conflict_action = 'ignore';
SYSTEM STOP MERGES uk_ig_mix;

INSERT INTO uk_ig_mix VALUES (1, 'a'), (2, 'b');
INSERT INTO uk_ig_mix
    VALUES (1, 'a_new'), (3, 'c'), (4, 'd');
SELECT 'ignore_count', count() FROM uk_ig_mix;  -- 4
SELECT 'ignore_rows', id, v FROM uk_ig_mix ORDER BY id;  -- 1 a / 2 b / 3 c / 4 d

-- 1b. ignore, partitioned: red if the rewrite takes the partition from the sink's block
-- (LOGICAL_ERROR) instead of the written part.
CREATE TABLE uk_ig_part (id UInt32, v String)
ENGINE = MergeTree PARTITION BY id % 2 ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, unique_key_conflict_action = 'ignore';
SYSTEM STOP MERGES uk_ig_part;

INSERT INTO uk_ig_part VALUES (2, 'b');
INSERT INTO uk_ig_part VALUES (2, 'b_new'), (4, 'd');
SELECT 'ignore_partitioned', _partition_id, id, v FROM uk_ig_part ORDER BY id;  -- 0 2 b / 0 4 d

DROP TABLE uk_ig_mix;
DROP TABLE uk_ig_part;
