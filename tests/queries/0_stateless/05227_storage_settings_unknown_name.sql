-- Tags: no-fasttest
-- Tag justification:
--   no-fasttest: the `S3` engine is not registered in the fast test build, which configures
--                `-DENABLE_LIBRARIES=0` (its registration is under `#if USE_AWS_S3`).

DROP TABLE IF EXISTS t_unknown_setting;

SELECT '--- a name that is not a setting at all is rejected ---';

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV)
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = URL('http://localhost:1/x.csv', CSV)
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = S3('http://localhost:11111/test/u.csv', NOSIGN, 'CSV')
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = COSN('http://localhost:11111/test/u.csv', NOSIGN, 'CSV')
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = OSS('http://localhost:11111/test/u.csv', NOSIGN, 'CSV')
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = GCS('http://localhost:11111/test/u.csv', NOSIGN, 'CSV')
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

-- `AzureBlobStorage` and `HDFS` are registered apart from the S3-compatible names above, so each of
-- the two carries its own check.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = AzureBlobStorage('http://localhost:11111/test', 'cont', 'u.csv', 'CSV')
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = HDFS('hdfs://localhost:12222/u.csv', 'CSV')
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = Log
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = StripeLog
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

-- The typo the report is about: `format_csv_delimiter` is a setting, `input_format_csv_delimiter` is not.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV)
SETTINGS input_format_csv_delimiter = '|'; -- { serverError UNKNOWN_SETTING }

-- `name = DEFAULT` is parsed into a different payload of the SETTINGS clause than `name = value`.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog
SETTINGS disk = 'default', not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV)
SETTINGS format_csv_delimiter = ';', not_a_setting_at_all = DEFAULT; -- { serverError UNKNOWN_SETTING }

-- `param_x = ...` is parsed into a third payload of the SETTINGS clause, which only a standalone `SET` reads.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog
SETTINGS disk = 'default', param_x = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV)
SETTINGS format_csv_delimiter = ';', param_x = 1; -- { serverError UNKNOWN_SETTING }

-- On the `AS SELECT` path the split does not run, so this payload reaches the check there too.
CREATE TABLE t_unknown_setting ENGINE = TinyLog
SETTINGS param_x = 1 AS SELECT 1 AS a; -- { serverError UNKNOWN_SETTING }

SELECT '--- a setting of the engine is still accepted ---';

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog SETTINGS disk = 'default';
SHOW CREATE TABLE t_unknown_setting;
DROP TABLE t_unknown_setting;

-- A reset naming a query setting is accepted. Where the reset itself ends up is decided by the
-- SETTINGS split, not by this check, so only the engine setting is read back.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog SETTINGS disk = 'default', max_threads = DEFAULT;
SELECT create_table_query LIKE '%SETTINGS disk = \'default\'%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_unknown_setting';
DROP TABLE t_unknown_setting;

-- A query parameter is declared by `SET` and substituted into a setting value, which is a value and not a name.
SET param_d = ';';
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV) SETTINGS format_csv_delimiter = {d:String};
SHOW CREATE TABLE t_unknown_setting;
DROP TABLE t_unknown_setting;

SELECT '--- a query setting is still accepted, and is not stored on the table ---';

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog SETTINGS max_threads = 3;
SHOW CREATE TABLE t_unknown_setting;
DROP TABLE t_unknown_setting;

-- An alias of a query setting resolves, so it must not be mistaken for an unknown name.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog SETTINGS enable_analyzer = 1;
SHOW CREATE TABLE t_unknown_setting;
DROP TABLE t_unknown_setting;

SELECT '--- a custom setting the context holds is a query setting too ---';

SET custom_x = 1;

-- A held custom name is a query setting, so a reset naming it is accepted like any other.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog SETTINGS disk = 'default', custom_x = DEFAULT;
SELECT create_table_query LIKE '%SETTINGS disk = \'default\'%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_unknown_setting';
DROP TABLE t_unknown_setting;

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV) SETTINGS format_csv_delimiter = ';', custom_x = DEFAULT;
SELECT create_table_query LIKE '%SETTINGS format_csv_delimiter = \';\'%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_unknown_setting';
DROP TABLE t_unknown_setting;

-- `CREATE ... AS SELECT` skips the move entirely, so `name = value` reaches the check unsplit as well.
CREATE TABLE t_unknown_setting ENGINE = TinyLog SETTINGS disk = 'default', custom_x = 1 AS SELECT 1 AS a;
SHOW CREATE TABLE t_unknown_setting;
DROP TABLE t_unknown_setting;

-- The same path leaves a reset unsplit too, which is where an accepted reset is read back in full.
CREATE TABLE t_unknown_setting ENGINE = TinyLog SETTINGS disk = 'default', custom_x = DEFAULT AS SELECT 1 AS a;
SHOW CREATE TABLE t_unknown_setting;
DROP TABLE t_unknown_setting;

-- A custom name the context does not hold is not a query setting: the move leaves it behind and no
-- engine reads it, which is the case this check is for.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog
SETTINGS disk = 'default', custom_not_held_anywhere = 1; -- { serverError UNKNOWN_SETTING }

SELECT '--- a bad value is still reported as a bad value, not as an unknown name ---';

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV)
SETTINGS format_csv_delimiter = 'more_than_one_char'; -- { serverError SIZE_OF_FIXED_STRING_DOESNT_MATCH }

DROP TABLE IF EXISTS t_unknown_setting;
