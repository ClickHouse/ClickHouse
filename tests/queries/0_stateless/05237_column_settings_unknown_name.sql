-- A column `SETTINGS` clause has three payloads: `name = value`, `name = DEFAULT` and `param_name = ...`.
-- Only the first one used to reach the name check, so the other two accepted any name and dropped it.
-- `min_compress_block_size` and `max_compress_block_size` are the only names settable per column.

DROP TABLE IF EXISTS t_column_settings_unknown;

SELECT '-- CREATE --';

CREATE TABLE t_column_settings_unknown (x UInt64 SETTINGS (not_a_setting = 1)) ENGINE = MergeTree ORDER BY x; -- { serverError UNKNOWN_SETTING }
CREATE TABLE t_column_settings_unknown (x UInt64 SETTINGS (not_a_setting = DEFAULT)) ENGINE = MergeTree ORDER BY x; -- { serverError UNKNOWN_SETTING }
CREATE TABLE t_column_settings_unknown (x UInt64 SETTINGS (param_not_a_setting = 1)) ENGINE = MergeTree ORDER BY x; -- { serverError UNKNOWN_SETTING }

-- The prefixed spelling of a settable name is not a setting name either: nothing hoists a name out of it.
CREATE TABLE t_column_settings_unknown (x UInt64 SETTINGS (param_min_compress_block_size = 1)) ENGINE = MergeTree ORDER BY x; -- { serverError UNKNOWN_SETTING }

-- The rule is on the name, so any engine that keeps the declaration is screened. `TimeSeries` is not:
-- its normalizer regenerates the outer columns, dropping every column clause whatever the spelling.
CREATE TABLE t_column_settings_unknown (x UInt64 SETTINGS (not_a_setting = DEFAULT)) ENGINE = Log; -- { serverError UNKNOWN_SETTING }

SELECT '-- TimeSeries regenerates its outer columns, so no spelling is screened there --';

SET allow_experimental_time_series_table = 1;
CREATE TABLE t_column_settings_ts (metric_name String SETTINGS (not_a_setting = 1), tags Map(String, String)) ENGINE = TimeSeries;
DROP TABLE t_column_settings_ts;
CREATE TABLE t_column_settings_ts (metric_name String SETTINGS (not_a_setting = DEFAULT), tags Map(String, String)) ENGINE = TimeSeries;
DROP TABLE t_column_settings_ts;
CREATE TABLE t_column_settings_ts (metric_name String SETTINGS (param_not_a_setting = 1), tags Map(String, String)) ENGINE = TimeSeries;
SELECT position(create_table_query, 'SETTINGS (') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_column_settings_ts';
DROP TABLE t_column_settings_ts;

CREATE TABLE t_column_settings_unknown (x UInt64, y UInt64, c_with_settings UInt64 SETTINGS (min_compress_block_size = 100))
ENGINE = MergeTree ORDER BY x;

-- Naming the column distinctly keeps the assertion off the table-level clause, which carries the same
-- two setting names and may hold a randomization-injected entry.
SELECT create_table_query LIKE '%`c_with_settings` UInt64 SETTINGS (min_compress_block_size = 100)%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_column_settings_unknown';

SELECT '-- ADD COLUMN --';

ALTER TABLE t_column_settings_unknown ADD COLUMN a UInt64 SETTINGS (not_a_setting = 1); -- { serverError UNKNOWN_SETTING }
ALTER TABLE t_column_settings_unknown ADD COLUMN a UInt64 SETTINGS (not_a_setting = DEFAULT); -- { serverError UNKNOWN_SETTING }
ALTER TABLE t_column_settings_unknown ADD COLUMN a UInt64 SETTINGS (param_not_a_setting = 1); -- { serverError UNKNOWN_SETTING }

SELECT '-- MODIFY COLUMN ... SETTINGS (...) --';

ALTER TABLE t_column_settings_unknown MODIFY COLUMN y UInt64 SETTINGS (not_a_setting = 1); -- { serverError UNKNOWN_SETTING }
ALTER TABLE t_column_settings_unknown MODIFY COLUMN y UInt64 SETTINGS (not_a_setting = DEFAULT); -- { serverError UNKNOWN_SETTING }
ALTER TABLE t_column_settings_unknown MODIFY COLUMN y UInt64 SETTINGS (param_not_a_setting = 1); -- { serverError UNKNOWN_SETTING }

SELECT '-- MODIFY COLUMN ... MODIFY SETTING --';

ALTER TABLE t_column_settings_unknown MODIFY COLUMN y MODIFY SETTING not_a_setting = 1; -- { serverError UNKNOWN_SETTING }
ALTER TABLE t_column_settings_unknown MODIFY COLUMN y MODIFY SETTING not_a_setting = DEFAULT; -- { serverError UNKNOWN_SETTING }
ALTER TABLE t_column_settings_unknown MODIFY COLUMN y MODIFY SETTING param_not_a_setting = 1; -- { serverError UNKNOWN_SETTING }

SELECT '-- MODIFY COLUMN ... RESET SETTING --';

ALTER TABLE t_column_settings_unknown MODIFY COLUMN y RESET SETTING not_a_setting; -- { serverError UNKNOWN_SETTING }

SELECT '-- an ignored IF [NOT] EXISTS command stays a no-op, in every payload --';

ALTER TABLE t_column_settings_unknown ADD COLUMN IF NOT EXISTS x UInt64 SETTINGS (not_a_setting = 1);
ALTER TABLE t_column_settings_unknown ADD COLUMN IF NOT EXISTS x UInt64 SETTINGS (not_a_setting = DEFAULT);
ALTER TABLE t_column_settings_unknown ADD COLUMN IF NOT EXISTS x UInt64 SETTINGS (param_not_a_setting = 1);
ALTER TABLE t_column_settings_unknown MODIFY COLUMN IF EXISTS nosuch UInt64 SETTINGS (not_a_setting = DEFAULT);
ALTER TABLE t_column_settings_unknown MODIFY COLUMN IF EXISTS nosuch RESET SETTING not_a_setting;

-- A command that does take effect is screened even next to an ignored one in the same statement.
ALTER TABLE t_column_settings_unknown ADD COLUMN z UInt64, ADD COLUMN IF NOT EXISTS z UInt64 SETTINGS (not_a_setting = DEFAULT); -- { serverError UNKNOWN_SETTING }

SELECT '-- every reset spelling still resets a settable name --';

ALTER TABLE t_column_settings_unknown MODIFY COLUMN c_with_settings UInt64 SETTINGS (min_compress_block_size = DEFAULT);
SELECT create_table_query LIKE '%`c_with_settings` UInt64 SETTINGS%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_column_settings_unknown';

ALTER TABLE t_column_settings_unknown MODIFY COLUMN c_with_settings UInt64 SETTINGS (min_compress_block_size = 100);
ALTER TABLE t_column_settings_unknown MODIFY COLUMN c_with_settings MODIFY SETTING min_compress_block_size = DEFAULT;
SELECT create_table_query LIKE '%`c_with_settings` UInt64 SETTINGS%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_column_settings_unknown';

ALTER TABLE t_column_settings_unknown MODIFY COLUMN c_with_settings UInt64 SETTINGS (min_compress_block_size = 100);
ALTER TABLE t_column_settings_unknown MODIFY COLUMN c_with_settings RESET SETTING min_compress_block_size;
SELECT create_table_query LIKE '%`c_with_settings` UInt64 SETTINGS%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_column_settings_unknown';

SELECT '-- a short ATTACH replays a stored definition and is not screened --';

ALTER TABLE t_column_settings_unknown MODIFY COLUMN c_with_settings UInt64 SETTINGS (max_compress_block_size = 200);
DETACH TABLE t_column_settings_unknown;
ATTACH TABLE t_column_settings_unknown;
SELECT create_table_query LIKE '%`c_with_settings` UInt64 SETTINGS (max_compress_block_size = 200)%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_column_settings_unknown';

DROP TABLE t_column_settings_unknown;
