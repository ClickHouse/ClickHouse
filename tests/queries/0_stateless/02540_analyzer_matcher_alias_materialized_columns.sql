SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value_alias ALIAS concat('AliasValue_', toString(id)),
    value_materialized MATERIALIZED concat('MaterializedValue_', toString(id))
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0);

-- { echoOn }

SELECT * FROM test_table AS test_table_alias;

SELECT test_table_alias.* FROM test_table AS test_table_alias;

SELECT * FROM test_table AS test_table_alias SETTINGS asterisk_include_alias_columns = 1;

SELECT test_table_alias.* FROM test_table AS test_table_alias SETTINGS asterisk_include_alias_columns = 1;

SELECT * FROM test_table AS test_table_alias SETTINGS asterisk_include_materialized_columns = 1;

SELECT test_table_alias.* FROM test_table AS test_table_alias SETTINGS asterisk_include_materialized_columns = 1;

SELECT * FROM test_table AS test_table_alias SETTINGS asterisk_include_alias_columns = 1, asterisk_include_materialized_columns = 1;

SELECT test_table_alias.* FROM test_table AS test_table_alias SETTINGS asterisk_include_alias_columns = 1, asterisk_include_materialized_columns = 1;

-- { echoOff }

DROP TABLE test_table;
