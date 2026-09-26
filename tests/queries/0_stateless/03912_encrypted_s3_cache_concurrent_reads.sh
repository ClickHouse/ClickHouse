#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

# Test for race conditions in ReadBufferFromEncryptedFile when multiple
# concurrent readers access the same encrypted data through the filesystem cache.
# CachedOnDiskReadBufferFromFile can share file segments between readers,
# which may cause use-after-free in the encrypted buffer chain.

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_encrypted_s3_cache_race_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE} (key UInt64, value String)
    ENGINE = MergeTree ORDER BY key
    SETTINGS storage_policy = 's3_cache_encrypted', min_bytes_for_wide_part = 0;
    INSERT INTO ${TABLE} SELECT number, randomPrintableASCII(200) FROM numbers(50000);
    SYSTEM DROP FILESYSTEM CACHE 's3_cache';
"

TIMEOUT=10

function thread_read {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "
            SELECT count() FROM ${TABLE} WHERE NOT ignore(value) FORMAT Null
        " 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' -e '^\s*)$' | grep -v -e UNKNOWN_TABLE
    done
}

function thread_read_with_cache_drop {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE 's3_cache'" 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' -e '^\s*)$'
        $CLICKHOUSE_CLIENT --query "
            SELECT sum(key) FROM ${TABLE} WHERE NOT ignore(value) FORMAT Null
        " 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' -e '^\s*)$' | grep -v -e UNKNOWN_TABLE
    done
}

# Launch concurrent readers to trigger file segment sharing in the cache layer.
thread_read &
thread_read &
thread_read &
thread_read &
thread_read_with_cache_drop &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE}"

echo "OK"
