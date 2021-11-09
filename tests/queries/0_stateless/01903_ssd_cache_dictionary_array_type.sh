#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -n --query="
    DROP TABLE IF EXISTS dictionary_array_source_table;
    CREATE TABLE dictionary_array_source_table
    (
        id UInt64,
        array_value Array(Int64)
    ) ENGINE=TinyLog;

    INSERT INTO dictionary_array_source_table VALUES (0, [0, 1, 2]);

    DROP DICTIONARY IF EXISTS ssd_cache_dictionary;
    CREATE DICTIONARY ssd_cache_dictionary
    (
        id UInt64,
        array_value Array(Int64) DEFAULT [1,2,3]
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_array_source_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));

    SELECT 'SSDCache dictionary';
    SELECT dictGet('ssd_cache_dictionary', 'array_value', toUInt64(0));
    SELECT dictGet('ssd_cache_dictionary', 'array_value', toUInt64(1));
    SELECT dictGetOrDefault('ssd_cache_dictionary', 'array_value', toUInt64(1), [2,3,4]);
    DROP DICTIONARY ssd_cache_dictionary;
    DROP TABLE dictionary_array_source_table;"
