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

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = Log
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = StripeLog
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

-- The typo the report is about: `format_csv_delimiter` is a setting, `input_format_csv_delimiter` is not.
CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV)
SETTINGS input_format_csv_delimiter = '|'; -- { serverError UNKNOWN_SETTING }

SELECT '--- a setting of the engine is still accepted ---';

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = TinyLog SETTINGS disk = 'default';
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

SELECT '--- a bad value is still reported as a bad value, not as an unknown name ---';

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = File(CSV)
SETTINGS format_csv_delimiter = 'more_than_one_char'; -- { serverError SIZE_OF_FIXED_STRING_DOESNT_MATCH }

SELECT '--- MergeTree is unchanged ---';

CREATE TABLE t_unknown_setting (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS not_a_setting_at_all = 1; -- { serverError UNKNOWN_SETTING }

DROP TABLE IF EXISTS t_unknown_setting;
