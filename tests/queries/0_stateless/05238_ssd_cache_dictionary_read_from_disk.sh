#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Keys that do not fit into the in-memory write buffer are flushed to disk,
# reading a single attribute of such keys must skip over the other ones.
$CLICKHOUSE_CLIENT --query="
    CREATE TABLE source_table (id UInt64, a1 UInt64, a2 UInt64, a3 String, a4 Nullable(UInt64)) ENGINE = TinyLog;
    INSERT INTO source_table SELECT number, number + 1, number + 2, toString(number), if(number % 2, NULL, number) FROM numbers(500);

    CREATE DICTIONARY ssd_cache_dictionary (id UInt64, a1 UInt64, a2 UInt64, a3 String, a4 Nullable(UInt64))
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source_table'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(SSD_CACHE(FILE_SIZE 1048576 PATH '$CLICKHOUSE_USER_FILES/${CLICKHOUSE_DATABASE}_ssd_dic'));

    SELECT countIf(dictGet('ssd_cache_dictionary', 'a1', number) != number + 1) FROM numbers(500);
    SELECT countIf(dictGet('ssd_cache_dictionary', 'a2', number) != number + 2) FROM numbers(500);
    SELECT countIf(dictGet('ssd_cache_dictionary', 'a3', number) != toString(number)) FROM numbers(500);
    SELECT countIf(dictGet('ssd_cache_dictionary', 'a4', number) IS NOT NULL) FROM numbers(1, 500, 2);
    SELECT countIf(dictGet('ssd_cache_dictionary', ('a2', 'a4'), number) != (number + 2, number)) FROM numbers(0, 500, 2);

    DROP DICTIONARY ssd_cache_dictionary;
    DROP TABLE source_table;"
