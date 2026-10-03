-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: SYSTEM SCHEDULE MERGE and SYSTEM SYNC MERGES need the 'Manual' merge selector, and the change is not ported to SharedMergeTree yet.

-- Row TTL of a ReplacingMergeTree must not delete the newest version of a key while an older version of it
-- stays in another part of the partition (#122528): the older version would become visible again.
-- Such a TTL deletes rows only in TTL merges, which cover a whole partition; every other merge keeps the expired rows.
--
-- A TTL merge has priority over a merge scheduled with `SYSTEM SCHEDULE MERGE`. Scheduling a merge of the part with
-- the expired newest version and waiting with `SYSTEM SYNC MERGES` therefore waits until some merge - a TTL merge if
-- one can run, the scheduled one otherwise - has consumed that part. The tables keep empty parts
-- (`remove_empty_parts = 0`), because a removed empty part covers nothing, and `SYSTEM SYNC MERGES` would wait for it.

SET optimize_on_insert = 0;
SET optimize_throw_if_noop = 1;
-- `SYSTEM SYNC MERGES` waits forever with the default `max_execution_time = 0`.
SET max_execution_time = 120;

-- 1. The shape of #122528: a conditional TTL on a delete marker.
DROP TABLE IF EXISTS t_issue;
CREATE TABLE t_issue (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
TTL exp DELETE WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', merge_with_ttl_timeout = 0, remove_empty_parts = 0;

SYSTEM STOP TTL MERGES t_issue;
INSERT INTO t_issue VALUES (1, 1, 0, now() + INTERVAL 1 YEAR);
INSERT INTO t_issue VALUES (1, 2, 1, now() - INTERVAL 1 DAY);
SELECT 'issue before', k, v, d FROM t_issue FINAL;
SYSTEM START TTL MERGES t_issue;
SYSTEM SCHEDULE MERGE t_issue PARTS 'all_2_2_0';
SYSTEM SYNC MERGES t_issue;
SELECT 'issue older version visible', count() FROM t_issue FINAL WHERE v = 1;
OPTIMIZE TABLE t_issue FINAL;
SELECT 'issue after optimize', count() FROM t_issue FINAL;
DROP TABLE t_issue;

-- 2. An unconditional TTL on an ordinary row: the part of the newest version is dropped without being read.
DROP TABLE IF EXISTS t_drop;
CREATE TABLE t_drop (k UInt64, v UInt64, p String, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
TTL exp
SETTINGS merge_selector_algorithm = 'Manual', merge_with_ttl_timeout = 0, remove_empty_parts = 0;

SYSTEM STOP TTL MERGES t_drop;
INSERT INTO t_drop VALUES (1, 1, 'old', now() + INTERVAL 1 YEAR);
INSERT INTO t_drop VALUES (1, 2, 'new', now() - INTERVAL 1 DAY);
SYSTEM START TTL MERGES t_drop;
SYSTEM SCHEDULE MERGE t_drop PARTS 'all_2_2_0';
SYSTEM SYNC MERGES t_drop;
SELECT 'drop older version visible', count() FROM t_drop FINAL WHERE p = 'old';
OPTIMIZE TABLE t_drop FINAL;
SELECT 'drop after optimize', count() FROM t_drop FINAL;
DROP TABLE t_drop;

-- 3. A part where only the newest version of one key has expired: another key in it stays.
DROP TABLE IF EXISTS t_partial;
CREATE TABLE t_partial (k UInt64, v UInt64, p String, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
TTL exp
SETTINGS merge_selector_algorithm = 'Manual', merge_with_ttl_timeout = 0, remove_empty_parts = 0;

SYSTEM STOP TTL MERGES t_partial;
INSERT INTO t_partial VALUES (1, 1, 'old', now() + INTERVAL 1 YEAR);
INSERT INTO t_partial VALUES (1, 2, 'new', now() - INTERVAL 1 DAY), (2, 1, 'sentinel', now() + INTERVAL 1 YEAR);
SYSTEM START TTL MERGES t_partial;
SYSTEM SCHEDULE MERGE t_partial PARTS 'all_2_2_0';
SYSTEM SYNC MERGES t_partial;
SELECT 'partial older version visible', count() FROM t_partial FINAL WHERE p = 'old';
OPTIMIZE TABLE t_partial FINAL;
SELECT 'partial after optimize', k, p FROM t_partial FINAL ORDER BY k;
DROP TABLE t_partial;

-- 4. The engine's own `is_deleted` column does not protect a marker that the TTL deletes.
DROP TABLE IF EXISTS t_is_deleted;
CREATE TABLE t_is_deleted (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplacingMergeTree(v, d) ORDER BY k
TTL exp DELETE WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', merge_with_ttl_timeout = 0, remove_empty_parts = 0;

SYSTEM STOP TTL MERGES t_is_deleted;
INSERT INTO t_is_deleted VALUES (1, 1, 0, now() + INTERVAL 1 YEAR);
INSERT INTO t_is_deleted VALUES (1, 2, 1, now() - INTERVAL 1 DAY);
SYSTEM START TTL MERGES t_is_deleted;
SYSTEM SCHEDULE MERGE t_is_deleted PARTS 'all_2_2_0';
SYSTEM SYNC MERGES t_is_deleted;
SELECT 'is_deleted visible rows', count() FROM t_is_deleted FINAL;
OPTIMIZE TABLE t_is_deleted FINAL;
SELECT 'is_deleted stored rows', count() FROM t_is_deleted;
DROP TABLE t_is_deleted;

-- 5. A regular merge of the two newest of three versions. No TTL merge can run
-- (`max_number_of_merges_with_ttl_in_pool = 0`), so the scheduled merge is the one that runs.
DROP TABLE IF EXISTS t_subset;
CREATE TABLE t_subset (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
TTL exp DELETE WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', merge_with_ttl_timeout = 0, remove_empty_parts = 0, max_number_of_merges_with_ttl_in_pool = 0;

INSERT INTO t_subset VALUES (1, 1, 0, now() + INTERVAL 1 YEAR);
INSERT INTO t_subset VALUES (1, 2, 0, now() + INTERVAL 1 YEAR);
INSERT INTO t_subset VALUES (1, 3, 1, now() - INTERVAL 1 DAY);
SYSTEM SCHEDULE MERGE t_subset PARTS 'all_2_2_0', 'all_3_3_0';
SYSTEM SYNC MERGES t_subset;
-- The merged part keeps the expired marker, and its TTL info says so, so a TTL merge of the partition selects it.
SELECT 'subset parts', name, rows, rows_where_ttl_info.max[1] BETWEEN toDateTime(1) AND now()
FROM system.parts WHERE database = currentDatabase() AND table = 't_subset' AND active ORDER BY name;
SELECT 'subset winner', v, d FROM t_subset FINAL;
ALTER TABLE t_subset MODIFY SETTING max_number_of_merges_with_ttl_in_pool = 2;
-- A merge of the whole partition deletes the key: this one, or a background TTL merge that it waits for.
OPTIMIZE TABLE t_subset FINAL;
SELECT 'subset after optimize', count() FROM t_subset FINAL;
-- `DROP ... SYNC` waits for the merge task to finish, so that its `part_log` row is written.
DROP TABLE t_subset SYNC;
SYSTEM FLUSH LOGS part_log;
SELECT 'subset whole partition merge', merge_reason FROM system.part_log
WHERE database = currentDatabase() AND table = 't_subset' AND event_type = 'MergeParts' AND merged_from = ['all_1_1_0', 'all_2_3_1'];

-- 6. A merge that fills a column missing from its parts (added by `ADD COLUMN`) runs the TTL transform even with
-- TTL merges stopped. It must not delete the newest version either.
DROP TABLE IF EXISTS t_missing_column;
CREATE TABLE t_missing_column (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
TTL exp DELETE WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', remove_empty_parts = 0, enable_vertical_merge_algorithm = 0;

SYSTEM STOP TTL MERGES t_missing_column;
INSERT INTO t_missing_column VALUES (1, 1, 0, now() + INTERVAL 1 YEAR);
INSERT INTO t_missing_column VALUES (1, 2, 0, now() + INTERVAL 1 YEAR);
INSERT INTO t_missing_column VALUES (1, 3, 1, now() - INTERVAL 1 DAY);
ALTER TABLE t_missing_column ADD COLUMN extra UInt64;
SYSTEM SCHEDULE MERGE t_missing_column PARTS 'all_2_2_0', 'all_3_3_0';
SYSTEM SYNC MERGES t_missing_column;
SELECT 'missing column winner', v, d FROM t_missing_column FINAL;
DROP TABLE t_missing_column;
