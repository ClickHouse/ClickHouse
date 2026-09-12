-- `MODIFY SETTING name = DEFAULT` resets the setting, so it must pass the same engine checks as `RESET SETTING name`.

DROP TABLE IF EXISTS t_modify_setting_default_mt;
DROP TABLE IF EXISTS t_modify_setting_default_memory;
DROP TABLE IF EXISTS t_modify_setting_default_ts;

SELECT '-- MergeTree: resetting a setting which does not exist';
CREATE TABLE t_modify_setting_default_mt (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_with_ttl_timeout = 10;
ALTER TABLE t_modify_setting_default_mt RESET SETTING no_such_setting; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_setting_default_mt MODIFY SETTING no_such_setting = DEFAULT; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_setting_default_mt MODIFY SETTING merge_with_ttl_timeout = 20, no_such_setting = DEFAULT; -- { serverError BAD_ARGUMENTS }
SELECT extract(create_table_query, 'merge_with_ttl_timeout = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_setting_default_mt';

SELECT '-- Memory: the engine supports no reset at all';
CREATE TABLE t_modify_setting_default_memory (x UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 100;
ALTER TABLE t_modify_setting_default_memory RESET SETTING max_rows_to_keep; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_modify_setting_default_memory MODIFY SETTING max_rows_to_keep = DEFAULT; -- { serverError NOT_IMPLEMENTED }
SELECT extract(create_table_query, 'max_rows_to_keep = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_setting_default_memory';

SELECT '-- TimeSeries: the `version` setting is pinned at CREATE and cannot be dropped';
SET allow_experimental_time_series_table = 1;
CREATE TABLE t_modify_setting_default_ts ENGINE = TimeSeries;
ALTER TABLE t_modify_setting_default_ts RESET SETTING version; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_modify_setting_default_ts MODIFY SETTING version = DEFAULT; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_modify_setting_default_ts MODIFY SETTING filter_by_min_time_and_max_time = false, version = DEFAULT; -- { serverError NOT_IMPLEMENTED }
SELECT extract(create_table_query, 'version = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_setting_default_ts';

SELECT '-- an altered setting a reset does not touch still takes effect';
ALTER TABLE t_modify_setting_default_ts MODIFY SETTING filter_by_min_time_and_max_time = false, id_generator = DEFAULT;
SELECT extract(create_table_query, 'filter_by_min_time_and_max_time = (\\w+)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_setting_default_ts';

DROP TABLE t_modify_setting_default_mt;
DROP TABLE t_modify_setting_default_memory;
DROP TABLE t_modify_setting_default_ts;
