#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function run_test_once()
{
    $CLICKHOUSE_CLIENT -nm -q "
        DROP TABLE IF EXISTS 01863_simple_key_source_table;
        CREATE TABLE 01863_simple_key_source_table
        (
            id UInt64,
            value String
        ) ENGINE = TinyLog();

        INSERT INTO 01863_simple_key_source_table VALUES (1, 'First');
        INSERT INTO 01863_simple_key_source_table VALUES (1, 'First');

        CREATE DICTIONARY 01863_simple_key_cache_dictionary
        (
            id UInt64,
            value String
        )
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE '01863_simple_key_source_table'))
        LAYOUT(CACHE(SIZE_IN_CELLS 100000))
        LIFETIME(MIN 0 MAX 1000);
    "

    prev=$($CLICKHOUSE_CLIENT -nm -q "SELECT value FROM system.events WHERE event = 'DictCacheKeysRequestedMiss' SETTINGS system_events_show_zero_values=1")
    curr=$($CLICKHOUSE_CLIENT -nm -q "
        SELECT toUInt64(1) as key, dictGet('01863_simple_key_cache_dictionary', 'value', key) FORMAT Null;
        SELECT value FROM system.events WHERE event = 'DictCacheKeysRequestedMiss' SETTINGS system_events_show_zero_values=1
    ")

    $CLICKHOUSE_CLIENT -nm -q "
        DROP DICTIONARY 01863_simple_key_cache_dictionary;
    "

    if [ "$prev" == "$curr" ]; then
        echo OK
        return 0
    fi
    return 1
}
function main()
{
    # 200 retries since we look at global event DictCacheKeysRequestedMiss
    # NOTE: there is 100 runs of the particular test under flaky check
    for _ in {1..200}; do
        run_test_once && break
    done
}
main "$@"
