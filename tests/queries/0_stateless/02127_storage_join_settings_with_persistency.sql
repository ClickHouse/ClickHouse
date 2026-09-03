DROP TABLE IF EXISTS 02127_join_settings_with_persistency_1;
CREATE TABLE 02127_join_settings_with_persistency_1 (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent=1, join_any_take_last_row=0;
SHOW CREATE TABLE 02127_join_settings_with_persistency_1;
DROP TABLE IF EXISTS 02127_join_settings_with_persistency_0;
CREATE TABLE 02127_join_settings_with_persistency_0 (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent=0, join_any_take_last_row=0;
SHOW CREATE TABLE 02127_join_settings_with_persistency_0;
