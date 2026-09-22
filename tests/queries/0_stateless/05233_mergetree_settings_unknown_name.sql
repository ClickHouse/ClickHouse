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

SELECT '--- the replicated member of the family is judged the same way ---';

-- One `create` serves the whole family, and it judges the names before the engine resolves its
-- Keeper path, so none of these needs Keeper and the file stays runnable without one.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x
SETTINGS index_granularity = 4096, not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x
SETTINGS index_granularity = 4096, param_not_a_setting = 1; -- { serverError UNKNOWN_SETTING }

-- A setting of the engine is still not a name this rejects on the replicated engine either: this one
-- gets through the check and is refused by the family's own sanity check instead.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x
SETTINGS index_granularity = 0; -- { serverError BAD_ARGUMENTS }

SELECT '--- a setting of the engine is still accepted in the reset form ---';

-- Only the engine setting is read back: where an accepted reset ends up in the stored clause is
-- decided by the SETTINGS split, not by this check.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, min_bytes_for_wide_part = DEFAULT;
SELECT create_table_query LIKE '%SETTINGS index_granularity = 4096%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_mt_unknown_setting';
DROP TABLE t_mt_unknown_setting;

SELECT '--- an alias and an obsolete setting of the engine are still accepted ---';

-- `SHOW CREATE TABLE` reads the stored clause back exactly, and it also keeps the test runner from
-- randomizing MergeTree settings into these definitions, which is what makes that clause exact.

-- `allow_experimental_block_number_column` is an alias and `in_memory_parts_enable_wal` is obsolete;
-- both resolve to a setting of the engine, so neither is a name this rejects.
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, allow_experimental_block_number_column = 1, in_memory_parts_enable_wal = 1;
SHOW CREATE TABLE t_mt_unknown_setting;
DROP TABLE t_mt_unknown_setting;

SELECT '--- a query setting is still accepted ---';

CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, max_threads = DEFAULT;
SELECT create_table_query LIKE '%SETTINGS index_granularity = 4096%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_mt_unknown_setting';
DROP TABLE t_mt_unknown_setting;

SELECT '--- a query parameter is a VALUE, not a name, and still substitutes ---';

SET param_g = 4096;
CREATE TABLE t_mt_unknown_setting (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = {g:UInt64};
SHOW CREATE TABLE t_mt_unknown_setting;

DROP TABLE IF EXISTS t_mt_unknown_setting;
