#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n --query="
    CREATE TABLE source_table
    (
    id UInt64,
    v1 String,
    v2 Nullable(String),
    v3 Nullable(UInt64)
    )
    ENGINE = TinyLog;

    INSERT INTO source_table VALUES (0, 'zero', 'zero', 0), (1, 'one', NULL, 1);

    CREATE DICTIONARY ssd_cache_dictionary
    (
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL,
    v3 Nullable(UInt64)
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$CLICKHOUSE_USER_FILES/${CLICKHOUSE_DATABASE}_ssd_dic'));

    SELECT dictGetOrDefault('ssd_cache_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id))) FROM source_table;
    SELECT dictGetOrDefault('ssd_cache_dictionary', 'v2', id+1, intDiv(NULL, id)) FROM source_table;
    SELECT dictGetOrDefault('ssd_cache_dictionary', 'v3', id+1, intDiv(NULL, id)) FROM source_table;

    DROP DICTIONARY ssd_cache_dictionary;
    DROP TABLE source_table;"
