#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A long-running INSERT that keeps committing parts must defer background merges:
# no merge may start before the insert commits its last part.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_defer_merges;
    CREATE TABLE t_defer_merges (n UInt64, s UInt8) ENGINE = MergeTree ORDER BY n
    SETTINGS min_insert_duration_to_defer_merges_ms = 100;
"

# One part per row, ~0.1 s apart: a 5+ second insert producing 50 tiny parts.
# Without deferral the merge selector would start merging them long before the insert ends.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_defer_merges SELECT number, sleepEachRow(0.1) FROM numbers(50)
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
             max_threads = 1, max_insert_threads = 1;
"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_defer_merges;"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log;"

$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_defer_merges' AND event_type = 'NewPart';
"

# No merge started before the last part of the insert was committed.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_defer_merges'
        AND event_type IN ('MergePartsStart', 'MergeParts')
        AND event_time_microseconds < (
            SELECT max(event_time_microseconds) FROM system.part_log
            WHERE database = currentDatabase() AND table = 't_defer_merges' AND event_type = 'NewPart');
"

# Merges resume once the insert finishes.
for _ in {1..300}
do
    parts=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_defer_merges' AND active")
    if [ "$parts" -lt 50 ]; then
        echo "merged"
        break
    fi
    sleep 0.2
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_defer_merges;"
