#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS simple_key_source_table_01863;
    CREATE TABLE simple_key_source_table_01863
    (
        id UInt64,
        value String
    ) ENGINE = TinyLog();

    INSERT INTO simple_key_source_table_01863 VALUES (1, 'First');
    INSERT INTO simple_key_source_table_01863 VALUES (1, 'First');

    CREATE DICTIONARY simple_key_cache_dictionary_01863
    (
        id UInt64,
        value String
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'simple_key_source_table_01863'))
    LAYOUT(CACHE(SIZE_IN_CELLS 100000))
    LIFETIME(MIN 0 MAX 1000);
"

# DictCacheKeysRequestedMiss is a ProfileEvent, so attribute it to this test's own dictGet query
# via query_log instead of the process-wide system.events counter. This is immune to concurrent
# dictionary activity from other tests.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -m --query_id "$query_id" -q "
    SELECT toUInt64(1) as key, dictGet('simple_key_cache_dictionary_01863', 'value', key) FORMAT Null;
"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
misses=$($CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['DictCacheKeysRequestedMiss']
    FROM system.query_log
    WHERE query_id = '$query_id' AND type = 'QueryFinish' AND current_database = currentDatabase()
")

$CLICKHOUSE_CLIENT -m -q "
    DROP DICTIONARY simple_key_cache_dictionary_01863;
    DROP TABLE simple_key_source_table_01863;
"

if [ "$misses" == "0" ]; then
    echo OK
else
    echo "FAIL: DictCacheKeysRequestedMiss=$misses"
fi
