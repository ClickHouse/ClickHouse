#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.overwrite_cache_sharded_publication"
writer_pid=""
reader_pid=""
drop_pid=""
reader_output=$(mktemp "$CLICKHOUSE_TMP/overwrite-cache-reader-XXXXXX")

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_before_commit" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_throw_during_publish" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_throw_during_index_build" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_during_index_build" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_during_lookup" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_after_lookup_catalog_snapshot" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_after_lookup_ids" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_after_drop_index_publication" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_before_rollback" >/dev/null 2>&1 ||:
    if [[ -n "$writer_pid" ]]; then
        wait "$writer_pid" >/dev/null 2>&1 ||:
    fi
    if [[ -n "$reader_pid" ]]; then
        wait "$reader_pid" >/dev/null 2>&1 ||:
    fi
    if [[ -n "$drop_pid" ]]; then
        wait "$drop_pid" >/dev/null 2>&1 ||:
    fi
    rm -f "$reader_output"
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
    INDEX (tag)
    SETTINGS max_memory_bytes = 10000000"

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

bytes_before_rollback=$($CLICKHOUSE_CLIENT -q "
    SELECT total_bytes
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'overwrite_cache_sharded_publication'")
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_throw_during_publish"
if $CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, toUInt8(number % 250 + 2), 4, 'rolled-back' FROM numbers(2500)" >/dev/null 2>&1; then
    echo "Expected injected publication failure" >&2
    exit 1
fi
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_throw_during_publish"
bytes_after_rollback=$($CLICKHOUSE_CLIENT -q "
    SELECT total_bytes
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'overwrite_cache_sharded_publication'")
if (( bytes_after_rollback <= bytes_before_rollback )); then
    echo "Rollback did not account retained vector or hash-bucket capacity" >&2
    exit 1
fi
$CLICKHOUSE_CLIENT -q "
    SELECT 'rollback-publication', countIf(version = 3), countIf(version = 4)
    FROM $table
    WHERE tag = 1"
$CLICKHOUSE_CLIENT -q "SELECT 'rollback-new-key', count() FROM $table WHERE key = 2499 AND tag = 251"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_throw_during_index_build"
if $CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD INDEX (key)" >/dev/null 2>&1; then
    echo "Expected injected lookup-index build failure" >&2
    exit 1
fi
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_throw_during_index_build"
$CLICKHOUSE_CLIENT -q "
    SELECT 'index-build-rollback', position(create_table_query, 'INDEX (key)')
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'overwrite_cache_sharded_publication'"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_during_index_build"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD INDEX (key)" >/dev/null &
writer_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_during_index_build PAUSE"
$CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (3000, 1, 1, 'index-catch-up')"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_during_index_build"
wait "$writer_pid"
writer_pid=""
$CLICKHOUSE_CLIENT -q "SELECT 'index-catch-up', payload FROM $table WHERE key = 3000"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_after_lookup_catalog_snapshot"
$CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (4000, 77, 1, 'drop-race')" >/dev/null &
writer_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_after_lookup_catalog_snapshot PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $table DROP INDEX (key)"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_after_lookup_catalog_snapshot"
wait "$writer_pid"
writer_pid=""
$CLICKHOUSE_CLIENT -q "SELECT 'drop-during-insert-prep', payload FROM $table WHERE tag = 77"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_after_lookup_catalog_snapshot"
$CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (4001, 78, 1, 'add-race')" >/dev/null &
writer_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_after_lookup_catalog_snapshot PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD INDEX (key)"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_after_lookup_catalog_snapshot"
wait "$writer_pid"
writer_pid=""
$CLICKHOUSE_CLIENT -q "SELECT 'add-during-insert-prep', payload FROM $table WHERE key = 4001"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_throw_during_publish"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_before_rollback"
$CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (5000, 79, 1, 'rollback-reader')" >/dev/null 2>&1 &
writer_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_before_rollback PAUSE"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_after_lookup_ids"
$CLICKHOUSE_CLIENT -q "SELECT 'rollback-old-reader', count() FROM $table WHERE tag = 79" > "$reader_output" &
reader_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_after_lookup_ids PAUSE"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_before_rollback"
if timeout 1 tail --pid="$writer_pid" -f /dev/null; then
    echo "Rollback completed before a reader released its entry identifier" >&2
    exit 1
fi
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_after_lookup_ids"
wait "$reader_pid"
reader_pid=""
if wait "$writer_pid"; then
    echo "Expected injected publication failure" >&2
    exit 1
fi
writer_pid=""
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_throw_during_publish"
cat "$reader_output"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_during_lookup"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_after_drop_index_publication"
$CLICKHOUSE_CLIENT -q "SELECT 'drop-old-reader', count() FROM $table WHERE tag = 1" > "$reader_output" &
reader_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_during_lookup PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $table DROP INDEX (tag)" >/dev/null &
drop_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_after_drop_index_publication PAUSE"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_during_lookup"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_after_drop_index_publication"
wait "$reader_pid"
reader_pid=""
wait "$drop_pid"
drop_pid=""
cat "$reader_output"
