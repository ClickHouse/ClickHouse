#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# no-fasttest: requires a cache disk.
# no-random-settings: randomized buffer sizes change the control-side reservation count.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

set -e

# Populate a cache-on-write table and return the number of cache state-lock
# reservation attempts the INSERT made (ProfileEvents['FilesystemCacheReserveAttempts']).
# The data is deterministic and uncompressed, so the count is reproducible.
function run() {
    local granule=$1
    local suffix=$2

    local table="test_04409_${suffix}"
    local cache_path="04409_reserve_granularity_${CLICKHOUSE_DATABASE}_${suffix}"
    local qid="04409_${CLICKHOUSE_DATABASE}_${suffix}"

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table"
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $table (key UInt64, value String CODEC(NONE))
        ENGINE = MergeTree() ORDER BY key
        SETTINGS disk = disk(
            type = cache,
            name = '$cache_path',
            path = '$cache_path',
            disk = 'local_disk',
            max_size = '10Gi',
            max_file_segment_size = '1Mi',
            boundary_alignment = '1Mi',
            background_download_threads = 0,
            cache_on_write_operations = 1,
            reserve_granularity = '$granule'),
        min_bytes_for_wide_part = 0"

    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES $table"

    local cache_name
    cache_name=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.disks WHERE cache_path LIKE '%$cache_path%'")
    drop_filesystem_cache "$cache_name" > /dev/null

    # Uncompressed (CODEC(NONE)) deterministic value, so the data spans many 1Mi
    # file segments and both runs produce an identical layout.
    $CLICKHOUSE_CLIENT --query_id "$qid" --enable_filesystem_cache_on_write_operations=1 -q "
        INSERT INTO $table SELECT number, repeat('x', 1000) FROM numbers(20000)"

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

    $CLICKHOUSE_CLIENT -q "
        SELECT ProfileEvents['FilesystemCacheReserveAttempts']
        FROM system.query_log
        WHERE query_id = '$qid' AND current_database = currentDatabase() AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

control_attempts=$(run 0 control)
granule_attempts=$(run '4Mi' granule)

# Reserve-ahead serves most reservations from the already-reserved short-circuit,
# so the INSERT takes the cache state lock fewer times.
$CLICKHOUSE_CLIENT -q "SELECT 'reserve-ahead reduces reservation attempts: ' || toString($granule_attempts < $control_attempts)"

# Data is intact in both cases.
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(key) FROM test_04409_control"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(key) FROM test_04409_granule"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_04409_control"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_04409_granule"
