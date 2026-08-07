#!/usr/bin/env bash

# Background small-segment compaction paces its passes through the delayed queue instead of rescheduling
# immediately, because each pass publishes a generation and takes an exclusive row lock for every row it
# moves. `background_compaction_min_interval_ms` is the floor between passes.
#
# Pass timing is not asserted here, because the compaction profile events are server-wide and a parallel
# test can move them. What is asserted is the plumbing and the correctness of both states: the setting is
# accepted and round-trips through the metadata, and a table whose next pass is still an hour away serves
# reads, deletions, replacements and a reload exactly like one that compacts at once. Test
# 04758_overwrite_cache_background_small_segment_compaction covers a pass actually completing.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

paced="${CLICKHOUSE_DATABASE}.overwrite_cache_pacing_paced"
prompt="${CLICKHOUSE_DATABASE}.overwrite_cache_pacing_prompt"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $paced" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $prompt" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

create_table()
{
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $1
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
            background_compaction_min_segment_count = 4,
            background_compaction_min_interval_ms = $2"

    # One segment per insert, so every one of these also asks for a compaction pass.
    for key in {1..8}; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO $1 VALUES ($key, 1, 1, 'value-$key')"
    done
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $prompt"
create_table "$prompt" 0
$CLICKHOUSE_CLIENT -q "
    SELECT 'interval in metadata', extract(engine_full, 'background_compaction_min_interval_ms = ([0-9]+)')
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'overwrite_cache_pacing_prompt'"
$CLICKHOUSE_CLIENT -q "SELECT 'unpaced rows', count(), sum(key) FROM $prompt WHERE tag = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $paced"
create_table "$paced" 3600000
$CLICKHOUSE_CLIENT -q "SELECT 'paced rows', count(), sum(key) FROM $paced WHERE tag = 1"
$CLICKHOUSE_CLIENT -q "SELECT 'paced total_rows', total_rows FROM system.tables
    WHERE database = currentDatabase() AND name = 'overwrite_cache_pacing_paced'"

$CLICKHOUSE_CLIENT -q "DELETE FROM $paced WHERE key = 3 AND tag = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO $paced VALUES (4, 1, 2, 'replaced-4')"
$CLICKHOUSE_CLIENT -q "SELECT 'after write', count(), sum(key) FROM $paced WHERE tag = 1"
$CLICKHOUSE_CLIENT -q "SELECT 'replaced', payload FROM $paced WHERE key = 4 AND tag = 1"

# The pending pass never ran, so the log still holds every small segment and replay must restore them all.
$CLICKHOUSE_CLIENT -q "DETACH TABLE $paced"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE $paced"
$CLICKHOUSE_CLIENT -q "SELECT 'after attach', count(), sum(key) FROM $paced WHERE tag = 1"
$CLICKHOUSE_CLIENT -q "SELECT 'replaced after attach', payload FROM $paced WHERE key = 4 AND tag = 1"
$CLICKHOUSE_CLIENT -q "SELECT 'deleted after attach', count() FROM $paced WHERE key = 3 AND tag = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE $paced"
$CLICKHOUSE_CLIENT -q "DROP TABLE $prompt"
