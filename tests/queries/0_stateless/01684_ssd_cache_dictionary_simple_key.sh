#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -n --query="
    DROP DATABASE IF EXISTS 01684_database_for_cache_dictionary;
    CREATE DATABASE 01684_database_for_cache_dictionary;

    CREATE TABLE 01684_database_for_cache_dictionary.simple_key_simple_attributes_source_table
    (
    id UInt64,
    value_first String,
    value_second String
    )
    ENGINE = TinyLog;

    INSERT INTO 01684_database_for_cache_dictionary.simple_key_simple_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
    INSERT INTO 01684_database_for_cache_dictionary.simple_key_simple_attributes_source_table VALUES(1, 'value_1', 'value_second_1');
    INSERT INTO 01684_database_for_cache_dictionary.simple_key_simple_attributes_source_table VALUES(2, 'value_2', 'value_second_2');

    CREATE DICTIONARY 01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes
    (
    id UInt64,
    value_first String DEFAULT 'value_first_default',
    value_second String DEFAULT 'value_second_default'
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_simple_attributes_source_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));

    SELECT 'Dictionary cache_dictionary_simple_key_simple_attributes';
    SELECT 'dictGet existing value';
    SELECT dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
        dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
    SELECT 'dictGet with non existing value';
    SELECT dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
        dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
    SELECT 'dictGetOrDefault existing value';
    SELECT dictGetOrDefault('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_first', number, toString('default')) as value_first,
        dictGetOrDefault('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_second', number, toString('default')) as value_second FROM system.numbers LIMIT 3;
    SELECT 'dictGetOrDefault non existing value';
    SELECT dictGetOrDefault('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_first', number, toString('default')) as value_first,
        dictGetOrDefault('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_second', number, toString('default')) as value_second FROM system.numbers LIMIT 4;
    SELECT 'dictHas';
    SELECT dictHas('01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', number) FROM system.numbers LIMIT 4;
    SELECT 'select all values as input stream';
    SELECT * FROM 01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes ORDER BY id;

    DROP DICTIONARY 01684_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes;
    DROP TABLE 01684_database_for_cache_dictionary.simple_key_simple_attributes_source_table;

    CREATE TABLE 01684_database_for_cache_dictionary.simple_key_complex_attributes_source_table
    (
    id UInt64,
    value_first String,
    value_second Nullable(String)
    )
    ENGINE = TinyLog;

    INSERT INTO 01684_database_for_cache_dictionary.simple_key_complex_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
    INSERT INTO 01684_database_for_cache_dictionary.simple_key_complex_attributes_source_table VALUES(1, 'value_1', NULL);
    INSERT INTO 01684_database_for_cache_dictionary.simple_key_complex_attributes_source_table VALUES(2, 'value_2', 'value_second_2');

    CREATE DICTIONARY 01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes
    (
    id UInt64,
    value_first String DEFAULT 'value_first_default',
    value_second Nullable(String) DEFAULT 'value_second_default'
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_complex_attributes_source_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/1d'));

    SELECT 'Dictionary cache_dictionary_simple_key_complex_attributes';
    SELECT 'dictGet existing value';
    SELECT dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
        dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
    SELECT 'dictGet with non existing value';
    SELECT dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
        dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
    SELECT 'dictGetOrDefault existing value';
    SELECT dictGetOrDefault('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_first', number, toString('default')) as value_first,
        dictGetOrDefault('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_second', number, toString('default')) as value_second FROM system.numbers LIMIT 3;
    SELECT 'dictGetOrDefault non existing value';
    SELECT dictGetOrDefault('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_first', number, toString('default')) as value_first,
        dictGetOrDefault('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_second', number, toString('default')) as value_second FROM system.numbers LIMIT 4;
    SELECT 'dictHas';
    SELECT dictHas('01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', number) FROM system.numbers LIMIT 4;
    SELECT 'select all values as input stream';
    SELECT * FROM 01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes ORDER BY id;

    DROP DICTIONARY 01684_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes;
    DROP TABLE 01684_database_for_cache_dictionary.simple_key_complex_attributes_source_table;

    CREATE TABLE 01684_database_for_cache_dictionary.simple_key_hierarchy_table
    (
        id UInt64,
        parent_id UInt64
    ) ENGINE = TinyLog();

    INSERT INTO 01684_database_for_cache_dictionary.simple_key_hierarchy_table VALUES (1, 0);
    INSERT INTO 01684_database_for_cache_dictionary.simple_key_hierarchy_table VALUES (2, 1);
    INSERT INTO 01684_database_for_cache_dictionary.simple_key_hierarchy_table VALUES (3, 1);
    INSERT INTO 01684_database_for_cache_dictionary.simple_key_hierarchy_table VALUES (4, 2);

    CREATE DICTIONARY 01684_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy
    (
    id UInt64,
    parent_id UInt64 HIERARCHICAL
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_hierarchy_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/2d'));

    SELECT 'Dictionary cache_dictionary_simple_key_hierarchy';
    SELECT 'dictGet';
    SELECT dictGet('01684_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy', 'parent_id', number) FROM system.numbers LIMIT 5;
    SELECT 'dictGetHierarchy';
    SELECT dictGetHierarchy('01684_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy', toUInt64(1));
    SELECT dictGetHierarchy('01684_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy', toUInt64(4));

    DROP DICTIONARY 01684_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy;
    DROP TABLE 01684_database_for_cache_dictionary.simple_key_hierarchy_table;

    DROP DATABASE 01684_database_for_cache_dictionary;"
