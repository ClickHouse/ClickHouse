-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: SYSTEM SCHEDULE MERGE and SYSTEM SYNC MERGES need the 'Manual' merge selector, and the change is not ported to SharedMergeTree yet.

-- Which row TTLs of a ReplacingMergeTree need a merge of the whole partition to delete rows (#122528).
-- Each table has an older version of key 1 in `all_1_1_0` and a newer version, whose TTL has expired, in `all_2_2_0`.
-- A merge of `all_2_2_0` alone is scheduled while no TTL merge can run. The rows of its result show what it did:
-- 1 - it kept the expired row, because deleting it could make the older version visible again;
-- 0 - it deleted it, because an older version of a key expires no later than a newer one (or the setting allows it).
-- The tables keep empty parts (`remove_empty_parts = 0`): `SYSTEM SYNC MERGES` waits until a part covers the scheduled one.

SET optimize_on_insert = 0;
SET max_execution_time = 120;

DROP TABLE IF EXISTS key_only;
DROP TABLE IF EXISTS key_where_key;
DROP TABLE IF EXISTS key_where_other;
DROP TABLE IF EXISTS partition_key;
DROP TABLE IF EXISTS version_day;
DROP TABLE IF EXISTS version_hour_minute;
DROP TABLE IF EXISTS version_date_month;
DROP TABLE IF EXISTS version_where_other;
DROP TABLE IF EXISTS other_column;
DROP TABLE IF EXISTS function_of_version;
DROP TABLE IF EXISTS no_version;
DROP TABLE IF EXISTS other_column_opt_out;
DROP TABLE IF EXISTS plain_merge_tree;

-- The TTL reads only sorting key columns: every version of a key has the same TTL.
CREATE TABLE key_only (k UInt64, day Date, v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY (k, day) TTL day + INTERVAL 1 DAY
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO key_only VALUES (1, today() - 10, 1);
INSERT INTO key_only VALUES (1, today() - 10, 2);

CREATE TABLE key_where_key (k UInt64, day Date, v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY (k, day) TTL day + INTERVAL 1 DAY WHERE k = 1
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO key_where_key VALUES (1, today() - 10, 1);
INSERT INTO key_where_key VALUES (1, today() - 10, 2);

-- The TTL reads only a column that is the partition key: every row of a partition has the same TTL.
CREATE TABLE partition_key (k UInt64, day Date, v UInt64)
ENGINE = ReplacingMergeTree(v) PARTITION BY day ORDER BY k TTL day + INTERVAL 1 DAY
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO partition_key VALUES (1, '2000-01-01', 1);
INSERT INTO partition_key VALUES (1, '2000-01-01', 2);

-- The `WHERE` condition reads a column that differs between versions.
CREATE TABLE key_where_other (k UInt64, day Date, v UInt64, d UInt8)
ENGINE = ReplacingMergeTree(v) ORDER BY (k, day) TTL day + INTERVAL 1 DAY WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO key_where_other VALUES (1, today() - 10, 1, 0);
INSERT INTO key_where_other VALUES (1, today() - 10, 2, 1);

-- The TTL is the version plus positive intervals: an older version expires no later.
CREATE TABLE version_day (k UInt64, v DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k TTL v + INTERVAL 1 DAY
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO version_day VALUES (1, now() - INTERVAL 10 DAY);
INSERT INTO version_day VALUES (1, now() - INTERVAL 5 DAY);

CREATE TABLE version_hour_minute (k UInt64, v DateTime64(3))
ENGINE = ReplacingMergeTree(v) ORDER BY k TTL v + INTERVAL 1 HOUR + INTERVAL 1 MINUTE
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO version_hour_minute VALUES (1, now64(3) - INTERVAL 10 HOUR);
INSERT INTO version_hour_minute VALUES (1, now64(3) - INTERVAL 5 HOUR);

CREATE TABLE version_date_month (k UInt64, v Date)
ENGINE = ReplacingMergeTree(v) ORDER BY k TTL v + INTERVAL 1 MONTH
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO version_date_month VALUES (1, today() - 100);
INSERT INTO version_date_month VALUES (1, today() - 50);

-- The same TTL with a `WHERE` condition on a column that differs between versions.
CREATE TABLE version_where_other (k UInt64, v DateTime, d UInt8)
ENGINE = ReplacingMergeTree(v) ORDER BY k TTL v + INTERVAL 1 DAY WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO version_where_other VALUES (1, now() - INTERVAL 10 DAY, 0);
INSERT INTO version_where_other VALUES (1, now() - INTERVAL 5 DAY, 1);

-- The TTL reads a column that is not the version: its value can be later in an older version.
CREATE TABLE other_column (k UInt64, v UInt64, ts DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k TTL ts + INTERVAL 1 DAY
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO other_column VALUES (1, 1, now());
INSERT INTO other_column VALUES (1, 2, now() - INTERVAL 5 DAY);

-- A function of the version is not recognized.
CREATE TABLE function_of_version (k UInt64, v UInt32)
ENGINE = ReplacingMergeTree(v) ORDER BY k TTL toDateTime(v) + INTERVAL 1 DAY
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO function_of_version VALUES (1, toUInt32(now() - INTERVAL 10 DAY));
INSERT INTO function_of_version VALUES (1, toUInt32(now() - INTERVAL 5 DAY));

-- Without a version column the last inserted row wins.
CREATE TABLE no_version (k UInt64, ts DateTime)
ENGINE = ReplacingMergeTree ORDER BY k TTL ts + INTERVAL 1 DAY
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO no_version VALUES (1, now());
INSERT INTO no_version VALUES (1, now() - INTERVAL 5 DAY);

-- The setting restores the deletion in every merge.
CREATE TABLE other_column_opt_out (k UInt64, v UInt64, ts DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k TTL ts + INTERVAL 1 DAY
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0, replacing_ttl_whole_partition_only = 0;
INSERT INTO other_column_opt_out VALUES (1, 1, now());
INSERT INTO other_column_opt_out VALUES (1, 2, now() - INTERVAL 5 DAY);

-- Other engines are not affected.
CREATE TABLE plain_merge_tree (k UInt64, ts DateTime)
ENGINE = MergeTree ORDER BY k TTL ts + INTERVAL 1 DAY
SETTINGS merge_selector_algorithm = 'Manual', max_number_of_merges_with_ttl_in_pool = 0, remove_empty_parts = 0;
INSERT INTO plain_merge_tree VALUES (1, now());
INSERT INTO plain_merge_tree VALUES (1, now() - INTERVAL 5 DAY);

SYSTEM SCHEDULE MERGE key_only PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE key_where_key PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE key_where_other PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE partition_key PARTS '20000101_2_2_0';
SYSTEM SCHEDULE MERGE version_day PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE version_hour_minute PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE version_date_month PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE version_where_other PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE other_column PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE function_of_version PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE no_version PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE other_column_opt_out PARTS 'all_2_2_0';
SYSTEM SCHEDULE MERGE plain_merge_tree PARTS 'all_2_2_0';

SYSTEM SYNC MERGES key_only;
SYSTEM SYNC MERGES key_where_key;
SYSTEM SYNC MERGES key_where_other;
SYSTEM SYNC MERGES partition_key;
SYSTEM SYNC MERGES version_day;
SYSTEM SYNC MERGES version_hour_minute;
SYSTEM SYNC MERGES version_date_month;
SYSTEM SYNC MERGES version_where_other;
SYSTEM SYNC MERGES other_column;
SYSTEM SYNC MERGES function_of_version;
SYSTEM SYNC MERGES no_version;
SYSTEM SYNC MERGES other_column_opt_out;
SYSTEM SYNC MERGES plain_merge_tree;

SELECT table, rows FROM system.parts
WHERE database = currentDatabase() AND active AND name IN ('all_2_2_1', '20000101_2_2_1')
ORDER BY table;

DROP TABLE key_only;
DROP TABLE key_where_key;
DROP TABLE key_where_other;
DROP TABLE partition_key;
DROP TABLE version_day;
DROP TABLE version_hour_minute;
DROP TABLE version_date_month;
DROP TABLE version_where_other;
DROP TABLE other_column;
DROP TABLE function_of_version;
DROP TABLE no_version;
DROP TABLE other_column_opt_out;
DROP TABLE plain_merge_tree;
