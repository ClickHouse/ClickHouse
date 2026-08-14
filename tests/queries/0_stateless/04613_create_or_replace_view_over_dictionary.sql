-- The behavior of cross-kind replaces may change in the future, but each one must either succeed
-- or clearly fail before any change is committed, never leaving an orphan `_tmp_replace_*` object behind.

CREATE TABLE src (key String, value UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO src VALUES ('k1', 1);

CREATE DICTIONARY dict_then_view (key String, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(DIRECT());
SELECT dictGet(dict_then_view, 'value', 'k1');

CREATE OR REPLACE VIEW dict_then_view AS SELECT 42 AS value;
SELECT value FROM dict_then_view;

CREATE DICTIONARY dict_then_table (key String, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(DIRECT());
CREATE OR REPLACE TABLE dict_then_table (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dict_then_table VALUES (43);
SELECT x FROM dict_then_table;

CREATE DICTIONARY dict_then_dict (key String, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(DIRECT());
CREATE OR REPLACE DICTIONARY dict_then_dict (key String, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(DIRECT());
SELECT dictGet(dict_then_dict, 'value', 'k1');

SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name IN ('dict_then_view', 'dict_then_table');
SELECT count() FROM system.tables WHERE database = currentDatabase() AND startsWith(name, '_tmp_replace_');
