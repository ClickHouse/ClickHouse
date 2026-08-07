#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.overwrite_cache_lookup_tombstone_pruning"
reader_pid=""
reader_output=$(mktemp "$CLICKHOUSE_TMP/overwrite-cache-tombstones-XXXXXX")

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_during_lookup" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT overwrite_cache_pause_during_lookup" >/dev/null 2>&1 ||:
    if [[ -n "$reader_pid" ]]; then
        wait "$reader_pid" >/dev/null 2>&1 ||:
    fi
    rm -f "$reader_output"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE $table
    (
        key UInt64,
        tag UInt8,
        version UInt64,
        payload UInt8
    )
    ENGINE = OverwriteCache(version)
    KEYS (key, tag)
    INDEX (tag)
    SETTINGS max_memory_bytes = 100000000"

$CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, 1, 1, 1 FROM numbers(20000)"

# The lookup has captured its generation but has not copied the posting yet. Deletion must retain the
# posting until this reader finishes, or the old snapshot would lose every row.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT overwrite_cache_pause_during_lookup"
$CLICKHOUSE_CLIENT -q "SELECT 'old-reader', count(), sum(payload) FROM $table WHERE tag = 1" > "$reader_output" &
reader_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT overwrite_cache_pause_during_lookup PAUSE"

$CLICKHOUSE_CLIENT -q "DELETE FROM $table WHERE key IN (SELECT number FROM numbers(20000)) AND tag = 1"
bytes_with_tombstones=$($CLICKHOUSE_CLIENT -q "
    SELECT total_bytes
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'overwrite_cache_lookup_tombstone_pruning'")

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT overwrite_cache_pause_during_lookup"
wait "$reader_pid"
reader_pid=""
cat "$reader_output"

# The first new lookup runs deferred cleanup before registering its own snapshot.
$CLICKHOUSE_CLIENT -q "SELECT 'after-prune', count() FROM $table WHERE tag = 1"
bytes_after_prune=$($CLICKHOUSE_CLIENT -q "
    SELECT total_bytes
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'overwrite_cache_lookup_tombstone_pruning'")
if (( bytes_after_prune < bytes_with_tombstones )); then
    echo "posting memory reclaimed"
else
    echo "posting memory was not reclaimed"
fi

# A key resurrected after pruning reuses its entry identifier and restores sorted posting membership.
$CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (0, 1, 2, 7)"
$CLICKHOUSE_CLIENT -q "SELECT 'resurrected', payload FROM $table WHERE tag = 1"
