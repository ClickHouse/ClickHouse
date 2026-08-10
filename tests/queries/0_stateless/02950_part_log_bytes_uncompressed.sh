#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings
# Because we compare part sizes, and they could be affected by index granularity and index compression settings.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
CREATE TABLE part_log_bytes_uncompressed (
    key UInt8,
    value UInt8
)
Engine=MergeTree()
ORDER BY key"

$CLICKHOUSE_CLIENT -q "INSERT INTO part_log_bytes_uncompressed SELECT 1, 1 FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO part_log_bytes_uncompressed SELECT 2, 1 FROM numbers(1000)"

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE part_log_bytes_uncompressed FINAL"

$CLICKHOUSE_CLIENT -q "ALTER TABLE part_log_bytes_uncompressed UPDATE value = 3 WHERE 1 = 1 SETTINGS mutations_sync=2"

$CLICKHOUSE_CLIENT -q "INSERT INTO part_log_bytes_uncompressed SELECT 3, 1 FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE part_log_bytes_uncompressed DROP PART 'all_4_4_0' SETTINGS mutations_sync=2"

# Part removal is asynchronous: whichever cleanup pass grabs the part writes the RemovePart row,
# so it can land after DROP PART returns. START CLEANUP schedules a pass on every iteration, since
# a cleanup thread that found nothing to do sleeps up to max_cleanup_delay_period.
TIMEOUT=60
TIMELIMIT=$((SECONDS+TIMEOUT))
while [ $SECONDS -lt "$TIMELIMIT" ]
do
    $CLICKHOUSE_CLIENT -q "SYSTEM START CLEANUP part_log_bytes_uncompressed"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
    logged=$($CLICKHOUSE_CLIENT -q "
        SELECT count() > 0
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND database = currentDatabase() AND table = 'part_log_bytes_uncompressed'
            AND event_type = 'RemovePart' AND part_name = 'all_4_4_0'")
    if [ "$logged" = 1 ]
    then
        break
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "
SELECT event_type, table, part_name, bytes_uncompressed > 0, (bytes_uncompressed > 0 ? (size_in_bytes < bytes_uncompressed ? '1' : toString((size_in_bytes, bytes_uncompressed))) : '0')
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND database = currentDatabase() AND table = 'part_log_bytes_uncompressed'
    AND (event_type != 'RemovePart' OR part_name = 'all_4_4_0') -- ignore removal of other parts
ORDER BY part_name, event_type"

$CLICKHOUSE_CLIENT -q "DROP TABLE part_log_bytes_uncompressed"
