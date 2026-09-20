-- Tags: no-parallel

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_simple_attributes_source_table
(
   id UInt64,
   value_first String,
   value_second String
)
ENGINE = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_simple_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_simple_attributes_source_table VALUES(1, 'value_1', 'value_second_1');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_simple_attributes_source_table VALUES(2, 'value_2', 'value_second_2');

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.direct_dictionary_simple_key_simple_attributes
(
   id UInt64,
   value_first String DEFAULT 'value_first_default',
   value_second String DEFAULT 'value_second_default'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_simple_attributes_source_table'))
LAYOUT(DIRECT());

SELECT 'Dictionary direct_dictionary_simple_key_simple_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('direct_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
    dictGet('direct_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('direct_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
    dictGet('direct_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('direct_dictionary_simple_key_simple_attributes', 'value_first', number, toString('default')) as value_first,
    dictGetOrDefault('direct_dictionary_simple_key_simple_attributes', 'value_second', number, toString('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('direct_dictionary_simple_key_simple_attributes', 'value_first', number, toString('default')) as value_first,
    dictGetOrDefault('direct_dictionary_simple_key_simple_attributes', 'value_second', number, toString('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('direct_dictionary_simple_key_simple_attributes', number) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.direct_dictionary_simple_key_simple_attributes ORDER BY ALL;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.direct_dictionary_simple_key_simple_attributes;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_simple_attributes_source_table;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_complex_attributes_source_table
(
   id UInt64,
   value_first String,
   value_second Nullable(String)
)
ENGINE = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_complex_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_complex_attributes_source_table VALUES(1, 'value_1', NULL);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_complex_attributes_source_table VALUES(2, 'value_2', 'value_second_2');

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.direct_dictionary_simple_key_complex_attributes
(
   id UInt64,
   value_first String DEFAULT 'value_first_default',
   value_second Nullable(String) DEFAULT 'value_second_default'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_complex_attributes_source_table'))
LAYOUT(DIRECT());

SELECT 'Dictionary direct_dictionary_simple_key_complex_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('direct_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
    dictGet('direct_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('direct_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
    dictGet('direct_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('direct_dictionary_simple_key_complex_attributes', 'value_first', number, toString('default')) as value_first,
    dictGetOrDefault('direct_dictionary_simple_key_complex_attributes', 'value_second', number, toString('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('direct_dictionary_simple_key_complex_attributes', 'value_first', number, toString('default')) as value_first,
    dictGetOrDefault('direct_dictionary_simple_key_complex_attributes', 'value_second', number, toString('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('direct_dictionary_simple_key_complex_attributes', number) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.direct_dictionary_simple_key_complex_attributes ORDER BY ALL;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.direct_dictionary_simple_key_complex_attributes;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_complex_attributes_source_table;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_hierarchy_table
(
    id UInt64,
    parent_id UInt64
) ENGINE = TinyLog();

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_hierarchy_table VALUES (1, 0);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_hierarchy_table VALUES (2, 1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_hierarchy_table VALUES (3, 1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_hierarchy_table VALUES (4, 2);

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.direct_dictionary_simple_key_hierarchy
(
   id UInt64,
   parent_id UInt64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_hierarchy_table'))
LAYOUT(DIRECT());

SELECT 'Dictionary direct_dictionary_simple_key_hierarchy';
SELECT 'dictGet';
SELECT dictGet('direct_dictionary_simple_key_hierarchy', 'parent_id', number) FROM system.numbers LIMIT 5;
SELECT 'dictGetHierarchy';
SELECT dictGetHierarchy('direct_dictionary_simple_key_hierarchy', toUInt64(1));
SELECT dictGetHierarchy('direct_dictionary_simple_key_hierarchy', toUInt64(4));

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.direct_dictionary_simple_key_hierarchy;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_hierarchy_table;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
