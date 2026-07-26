#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.overwrite_cache_sharded_publication"
writer_pid=""

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_before_commit" >/dev/null 2>&1 ||:
    if [[ -n "$writer_pid" ]]; then
        wait "$writer_pid" >/dev/null 2>&1 ||:
    fi
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE $table
    (
        key UInt64,
        tag UInt8,
        version UInt64,
        payload String
    )
    ENGINE = OverwriteCache(version)
    KEYS (key, tag)
    SETTINGS
        max_memory_bytes = 10000000,
        secondary_index_columns = 'tag',
        max_secondary_index_rows = 3000"

$CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, 1, 1, 'old' FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_before_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, 1, 2, 'new' FROM numbers(2000)" >/dev/null &
writer_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_before_commit PAUSE"

timeout 10 $CLICKHOUSE_CLIENT -q "
    SELECT 'pending-primary', countIf(version = 1), countIf(version = 2)
    FROM $table
    WHERE key IN (0, 999, 1000, 1999) AND tag = 1"
timeout 10 $CLICKHOUSE_CLIENT -q "
    SELECT 'pending-secondary', countIf(version = 1), countIf(version = 2)
    FROM $table
    WHERE tag = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_before_commit"
wait "$writer_pid"
writer_pid=""

$CLICKHOUSE_CLIENT -q "
    SELECT 'committed-primary', countIf(version = 1), countIf(version = 2)
    FROM $table
    WHERE key IN (0, 999, 1000, 1999) AND tag = 1"
$CLICKHOUSE_CLIENT -q "
    SELECT 'committed-secondary', countIf(version = 1), countIf(version = 2)
    FROM $table
    WHERE tag = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_before_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, 1, 3, 'newer' FROM numbers(2000)" >/dev/null &
writer_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_before_commit PAUSE"
timeout 10 $CLICKHOUSE_CLIENT -q "
    SELECT 'second-pending', countIf(version = 2), countIf(version = 3)
    FROM $table
    WHERE tag = 1"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_before_commit"
wait "$writer_pid"
writer_pid=""

$CLICKHOUSE_CLIENT -q "
    SELECT 'second-committed', countIf(version = 2), countIf(version = 3)
    FROM $table
    WHERE tag = 1"
