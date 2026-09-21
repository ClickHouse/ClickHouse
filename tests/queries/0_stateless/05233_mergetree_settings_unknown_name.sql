DROP TABLE IF EXISTS t_mt_unknown_setting;

SELECT '--- a name that is not a setting at all is rejected ---';

-- The ordinary spelling, which the MergeTree family already refused.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

-- `name = DEFAULT` is parsed into a different payload of the SETTINGS clause than `name = value`.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

-- `param_x = ...` is parsed into a third payload, which only a standalone `SET` reads.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, param_not_a_setting = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS param_not_a_setting = 1; -- { serverError UNKNOWN_SETTING }

SELECT '--- a setting of the engine is still accepted in the reset form ---';

-- `SHOW CREATE TABLE` reads the stored clause back exactly, and it also keeps the test runner from
-- randomizing MergeTree settings into these definitions, which is what makes that clause exact.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, min_bytes_for_wide_part = DEFAULT;
SHOW CREATE TABLE t_mt_unknown_setting;
DROP TABLE t_mt_unknown_setting;

SELECT '--- an alias and an obsolete setting of the engine are still accepted ---';

-- `allow_experimental_block_number_column` is an alias and `in_memory_parts_enable_wal` is obsolete;
-- both resolve to a setting of the engine, so neither is a name this rejects.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, allow_experimental_block_number_column = 1, in_memory_parts_enable_wal = 1;
SHOW CREATE TABLE t_mt_unknown_setting;
DROP TABLE t_mt_unknown_setting;

SELECT '--- a query setting is still accepted, and is not stored on the table ---';

CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, max_threads = DEFAULT;
SHOW CREATE TABLE t_mt_unknown_setting;
DROP TABLE t_mt_unknown_setting;

SELECT '--- a query parameter is a VALUE, not a name, and still substitutes ---';

SET param_g = 4096;
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = {g:UInt64};
SHOW CREATE TABLE t_mt_unknown_setting;

DROP TABLE IF EXISTS t_mt_unknown_setting;
