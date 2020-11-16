#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mt_compact"

# Checks that granularity correctly computed from small parts.

$CLICKHOUSE_CLIENT -q "CREATE TABLE mt_compact(a Int, s String) ENGINE = MergeTree ORDER BY a
                        SETTINGS min_rows_for_wide_part = 1000,
                        index_granularity = 14;"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES mt_compact"

$CLICKHOUSE_CLIENT --max_block_size=1 --min_insert_block_size_rows=1 -q \
    "INSERT INTO mt_compact SELECT number, 'aaa' FROM numbers(100);"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE table = 'mt_compact' AND database = currentDatabase() AND active"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES mt_compact"

# Retry because already started concurrent merges may interrupt optimize
for _ in {0..10}; do
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE mt_compact FINAL SETTINGS optimize_throw_if_noop=1" 2>/dev/null
    if [ $? -eq 0 ]; then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(marks) FROM system.parts WHERE table = 'mt_compact' AND database = currentDatabase() AND active"
$CLICKHOUSE_CLIENT -q  "DROP TABLE mt_compact"
