SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS merge_over_alias_with_missing_target;
CREATE TABLE IF NOT EXISTS target_for_alias_with_missing_target (`key` UInt32) ENGINE = MergeTree ORDER BY key;
DROP TABLE IF EXISTS alias_with_missing_target;
DROP TABLE IF EXISTS target_for_alias_with_missing_target;

CREATE TABLE target_for_alias_with_missing_target (`key` UInt32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE alias_with_missing_target ENGINE = Alias('target_for_alias_with_missing_target');
DROP TABLE target_for_alias_with_missing_target;

CREATE TABLE merge_over_alias_with_missing_target (`key` UInt32) ENGINE = Merge(currentDatabase(), '^alias_with_missing_target$');

SELECT * FROM merge_over_alias_with_missing_target; -- { serverError UNKNOWN_TABLE }
SELECT * FROM merge_over_alias_with_missing_target FINAL; -- { serverError UNKNOWN_TABLE }
SELECT * FROM merge_over_alias_with_missing_target SAMPLE 10; -- { serverError UNKNOWN_TABLE }

CREATE TABLE target_for_alias_with_missing_target (`key` UInt32) ENGINE = MergeTree ORDER BY key;

DROP TABLE merge_over_alias_with_missing_target;
DROP TABLE alias_with_missing_target;
DROP TABLE target_for_alias_with_missing_target;
