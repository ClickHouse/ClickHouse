#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

# Test for use-after-free in ReadBufferFromEncryptedFile when a query is
# cancelled mid-read. Destroying the reader chain from one thread while
# decryption is in progress on another can leave dangling buffer pointers.

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_encrypted_s3_cache_cancel_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE} (key UInt64, value String)
    ENGINE = MergeTree ORDER BY key
    SETTINGS storage_policy = 's3_cache_encrypted', min_bytes_for_wide_part = 0;
    INSERT INTO ${TABLE} SELECT number, randomPrintableASCII(500) FROM numbers(100000);
"

TIMEOUT=10

function thread_read_cancel {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # Start a heavy read and let it be killed by timeout.
        $CLICKHOUSE_CLIENT --max_execution_time 0.05 --query "
            SELECT * FROM ${TABLE} WHERE NOT ignore(value) FORMAT Null
        " 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' -e '^\s*)$' | grep -v -e UNKNOWN_TABLE -e TIMEOUT_EXCEEDED -e MEMORY_LIMIT_EXCEEDED -e QUERY_WAS_CANCELLED
    done
}

function thread_read_full {
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "
            SELECT count() FROM ${TABLE} WHERE NOT ignore(value) FORMAT Null
        " 2>&1 | grep -v -e 'Received exception from server' -e '^(query: ' -e '^\s*)$' | grep -v -e UNKNOWN_TABLE
    done
}

# Mix cancelled and full reads to trigger destruction races.
thread_read_cancel &
thread_read_cancel &
thread_read_cancel &
thread_read_full &
thread_read_full &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${TABLE}"

echo "OK"
