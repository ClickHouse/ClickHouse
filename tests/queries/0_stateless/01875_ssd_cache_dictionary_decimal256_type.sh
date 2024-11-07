#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="
    SET allow_experimental_bigint_types = 1;

    DROP TABLE IF EXISTS dictionary_decimal_source_table;
    CREATE TABLE dictionary_decimal_source_table
    (
        id UInt64,
        decimal_value Decimal256(5)
    ) ENGINE = TinyLog;

    INSERT INTO dictionary_decimal_source_table VALUES (1, 5.0);

    DROP DICTIONARY IF EXISTS ssd_cache_dictionary;
    CREATE DICTIONARY ssd_cache_dictionary
    (
        id UInt64,
        decimal_value Decimal256(5)
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_decimal_source_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));

    SELECT 'SSDCache dictionary';
    SELECT dictGet('ssd_cache_dictionary', 'decimal_value', toUInt64(1));

    DROP DICTIONARY ssd_cache_dictionary;"
