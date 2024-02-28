#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -n --query="
    DROP DATABASE IF EXISTS 02950_database_for_ssd_cache_dictionary;
    CREATE DATABASE 02950_database_for_ssd_cache_dictionary;

    CREATE TABLE 02950_database_for_ssd_cache_dictionary.source_table
    (
    id UInt64,
    v1 String,
    v2 Nullable(String),
    v3 Nullable(UInt64)
    )
    ENGINE = TinyLog;

    INSERT INTO 02950_database_for_ssd_cache_dictionary.source_table VALUES (0, 'zero', 'zero', 0), (1, 'one', NULL, 1);

    CREATE DICTIONARY 02950_database_for_ssd_cache_dictionary.ssd_cache_dictionary
    (
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL,
    v3 Nullable(UInt64)
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));

    SELECT dictGetOrDefault('02950_database_for_ssd_cache_dictionary.ssd_cache_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id))) FROM 02950_database_for_ssd_cache_dictionary.source_table;
    SELECT dictGetOrDefault('02950_database_for_ssd_cache_dictionary.ssd_cache_dictionary', 'v2', id+1, intDiv(NULL, id)) FROM 02950_database_for_ssd_cache_dictionary.source_table;
    SELECT dictGetOrDefault('02950_database_for_ssd_cache_dictionary.ssd_cache_dictionary', 'v3', id+1, intDiv(NULL, id)) FROM 02950_database_for_ssd_cache_dictionary.source_table;

    DROP DICTIONARY 02950_database_for_ssd_cache_dictionary.ssd_cache_dictionary;
    DROP TABLE 02950_database_for_ssd_cache_dictionary.source_table;
    DROP DATABASE 02950_database_for_ssd_cache_dictionary;"
