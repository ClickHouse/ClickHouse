DROP TABLE IF EXISTS src;
CREATE TABLE src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1, 42);

CREATE DICTIONARY d_unknown (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(not_a_setting_at_all = 1); -- { serverError UNKNOWN_SETTING }

CREATE DICTIONARY d_mixed (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(max_threads = 4, not_a_setting_at_all = 1); -- { serverError UNKNOWN_SETTING }

CREATE DICTIONARY d_param (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(param_not_a_setting = 1); -- { serverError UNKNOWN_SETTING }

CREATE DICTIONARY d_ok (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(max_result_bytes = 1000000);
SELECT 'query setting', dictGet('d_ok', 'v', toUInt64(1));

-- An alias, an obsolete setting and a format setting are all reachable through the settings accessor.
CREATE DICTIONARY d_accessor (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(insert_distributed_sync = 1, parallel_replicas_only_with_analyzer = 1, format_csv_allow_single_quotes = 1);
SELECT 'alias, obsolete, format', dictGet('d_accessor', 'v', toUInt64(1));

-- A prefixed name need not be held by the session already: applying the entry creates the setting.
CREATE DICTIONARY d_custom (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(custom_dict_probe = 1);
SELECT 'custom prefix', dictGet('d_custom', 'v', toUInt64(1));

-- `profile` is intercepted before the settings accessor, so it is not a settings entry at all.
CREATE DICTIONARY d_profile (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(profile = 'default');
SELECT 'profile', dictGet('d_profile', 'v', toUInt64(1));

CREATE DICTIONARY d_sql_prefix (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(SQL_dict_probe = 1);
SELECT 'registered prefix', dictGet('d_sql_prefix', 'v', toUInt64(1));

-- The registered prefixes are case-sensitive.
CREATE DICTIONARY d_lower_prefix (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(sql_dict_probe = 1); -- { serverError UNKNOWN_SETTING }

CREATE OR REPLACE DICTIONARY d_ok (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(not_a_setting_at_all = 1); -- { serverError UNKNOWN_SETTING }

DROP DICTIONARY d_ok;
DROP DICTIONARY d_accessor;
DROP DICTIONARY d_custom;
DROP DICTIONARY d_profile;
DROP DICTIONARY d_sql_prefix;
DROP TABLE src;
