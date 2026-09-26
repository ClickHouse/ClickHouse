DROP TABLE IF EXISTS t_alter_modify_setting_default;

CREATE TABLE t_alter_modify_setting_default (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 1024, merge_with_ttl_timeout = 10, max_bytes_to_merge_at_max_space_in_pool = 1;

SELECT '-- the settings after `CREATE`';
SELECT arrayStringConcat(arraySort(extractAll(create_table_query, '(?:merge_with_ttl_timeout|max_bytes_to_merge_at_max_space_in_pool) = \\d+')), ', ')
FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_modify_setting_default';

SELECT '-- `MODIFY SETTING name = DEFAULT` removes the setting from the clause';
ALTER TABLE t_alter_modify_setting_default MODIFY SETTING merge_with_ttl_timeout = DEFAULT;
SELECT arrayStringConcat(arraySort(extractAll(create_table_query, '(?:merge_with_ttl_timeout|max_bytes_to_merge_at_max_space_in_pool) = \\d+')), ', ')
FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_modify_setting_default';

SELECT '-- a reset and a change in one command';
ALTER TABLE t_alter_modify_setting_default MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = DEFAULT, merge_with_ttl_timeout = 20;
SELECT arrayStringConcat(arraySort(extractAll(create_table_query, '(?:merge_with_ttl_timeout|max_bytes_to_merge_at_max_space_in_pool) = \\d+')), ', ')
FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_modify_setting_default';

SELECT '-- resetting a setting which is not in the clause changes nothing';
ALTER TABLE t_alter_modify_setting_default MODIFY SETTING min_bytes_for_wide_part = DEFAULT;
SELECT arrayStringConcat(arraySort(extractAll(create_table_query, '(?:merge_with_ttl_timeout|max_bytes_to_merge_at_max_space_in_pool) = \\d+')), ', ')
FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_modify_setting_default';

SELECT '-- a setting cannot be both modified and reset, or reset twice, in one command';
ALTER TABLE t_alter_modify_setting_default MODIFY SETTING merge_with_ttl_timeout = 30, merge_with_ttl_timeout = DEFAULT; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alter_modify_setting_default MODIFY SETTING merge_with_ttl_timeout = DEFAULT, merge_with_ttl_timeout = 30; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alter_modify_setting_default MODIFY SETTING merge_with_ttl_timeout = DEFAULT, merge_with_ttl_timeout = DEFAULT; -- { serverError BAD_ARGUMENTS }
SELECT arrayStringConcat(arraySort(extractAll(create_table_query, '(?:merge_with_ttl_timeout|max_bytes_to_merge_at_max_space_in_pool) = \\d+')), ', ')
FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_modify_setting_default';

DROP TABLE t_alter_modify_setting_default;
