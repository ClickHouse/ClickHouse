#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.overwrite_cache_background_compaction"
invalid_table="${CLICKHOUSE_DATABASE}.overwrite_cache_background_compaction_invalid"
failpoint="overwrite_cache_pause_before_small_segment_compaction_publish"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $invalid_table" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

event_value()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT coalesce(any(value), 0)
        FROM system.events
        WHERE event = '$1'"
}

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
    SETTINGS
        max_memory_bytes = 100000000,
        persist_mode = 'sync',
        background_compaction_target_segment_bytes = 1000000,
        background_compaction_min_segment_count = 4"

compactions_before=$(event_value OverwriteCacheBackgroundCompactions)
segments_before=$(event_value OverwriteCacheBackgroundCompactedSegments)

# The compactor snapshots four small segments and builds their replacement outside the writer lock.
# Deleting one of those rows before publication must invalidate the whole plan; otherwise replaying the
# new segment after the deletion would resurrect that key.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $failpoint"
for key in 1 2 3 4; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES ($key, 1, 1, 'value-$key')"
done
timeout 10 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $failpoint PAUSE"

$CLICKHOUSE_CLIENT -q "DELETE FROM $table WHERE key = 1 AND tag = 1"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $failpoint"

# The failed validation leaves three eligible segments. A fourth one schedules a retry that can publish.
$CLICKHOUSE_CLIENT -q "INSERT INTO $table VALUES (5, 1, 1, 'value-5')"

compacted=0
for _ in {1..200}; do
    compactions_after=$(event_value OverwriteCacheBackgroundCompactions)
    segments_after=$(event_value OverwriteCacheBackgroundCompactedSegments)
    if (( compactions_after > compactions_before && segments_after >= segments_before + 4 )); then
        compacted=1
        break
    fi
    sleep 0.05
done

echo -e "compacted\t$compacted"
$CLICKHOUSE_CLIENT -q "
    SELECT 'before detach', count(), sum(key), countIf(version = 1)
    FROM $table
    WHERE tag = 1"
$CLICKHOUSE_CLIENT -q "SELECT 'deleted before detach', count() FROM $table WHERE key = 1 AND tag = 1"

$CLICKHOUSE_CLIENT -q "DETACH TABLE $table"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE $table"

$CLICKHOUSE_CLIENT -q "
    SELECT 'after attach', count(), sum(key), countIf(version = 1)
    FROM $table
    WHERE tag = 1"
$CLICKHOUSE_CLIENT -q "SELECT 'deleted after attach', count() FROM $table WHERE key = 1 AND tag = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE $table"

if $CLICKHOUSE_CLIENT -q "
    CREATE TABLE $invalid_table (key UInt64, version UInt64)
    ENGINE = OverwriteCache(version)
    KEYS (key)
    SETTINGS persist_mode = 'none', background_compaction_min_segment_count = 65" >/dev/null 2>&1; then
    echo -e "invalid setting rejected\t0"
else
    echo -e "invalid setting rejected\t1"
fi
