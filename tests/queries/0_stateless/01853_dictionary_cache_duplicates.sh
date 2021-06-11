#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function run_test_once()
{
    $CLICKHOUSE_CLIENT -nm -q "
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

    prev=$($CLICKHOUSE_CLIENT -nm -q "SELECT value FROM system.events WHERE event = 'DictCacheKeysRequestedMiss' SETTINGS system_events_show_zero_values=1")
    curr=$($CLICKHOUSE_CLIENT -nm -q "
        SELECT toUInt64(1) as key, dictGet('simple_key_cache_dictionary_01863', 'value', key) FORMAT Null;
        SELECT value FROM system.events WHERE event = 'DictCacheKeysRequestedMiss' SETTINGS system_events_show_zero_values=1
    ")

    $CLICKHOUSE_CLIENT -nm -q "
        DROP DICTIONARY simple_key_cache_dictionary_01863;
        DROP TABLE simple_key_source_table_01863;
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
