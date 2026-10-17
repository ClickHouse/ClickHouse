#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A regular merge of two parts that are still pending for a mutation re-weights them at the merged size,
# so `progress` stays 0 while the mutation has rewritten nothing (https://github.com/ClickHouse/ClickHouse/issues/114678).

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_merge_pending SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_merge_pending (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_merge_pending SELECT number, number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_merge_pending SELECT number, number FROM numbers(1000, 2000)"

# The mutation fails on every part, so it stays pending on both parts and on their merge.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mut_merge_pending UPDATE v = throwIf(v >= 0) WHERE 1" --mutations_sync=0

# The merge cannot take a part while a mutation attempt holds it, so retry until it lands.
for _ in {1..300}; do
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_mut_merge_pending FINAL" 2>/dev/null
    parts=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_mut_merge_pending' AND active")
    [[ "$parts" == "1" ]] && break
    sleep 0.1
done

# Read a moment with no attempt in flight, so no in-flight credit is counted.
for _ in {1..300}; do
    res=$($CLICKHOUSE_CLIENT -q "
        SELECT
            parts_to_do,
            bytes_to_do = (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = currentDatabase() AND table = 't_mut_merge_pending' AND active),
            progress
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_mut_merge_pending' AND NOT is_done AND empty(parts_in_progress_names)")
    [[ -n "$res" ]] && break
    sleep 0.1
done
echo "$res"

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_mut_merge_pending'" > /dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_merge_pending SYNC"
