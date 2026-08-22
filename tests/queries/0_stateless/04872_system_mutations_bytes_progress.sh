#!/usr/bin/env bash
# Tags: long, zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

# Tests for https://github.com/ClickHouse/ClickHouse/issues/114678: `system.mutations` exposes
# byte-weighted `bytes_to_do` and `progress`. With merges stopped no mutation task can start,
# so the numbers are exact: `bytes_to_do` equals the size of all active parts and `progress`
# is 0; after the mutation completes, the entry reports 0 bytes left and progress 1.

for engine in "MergeTree" "ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prog', 'r1')"; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_prog SYNC"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_prog (k UInt64, v UInt64) ENGINE = $engine ORDER BY k"
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_mut_prog"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_prog SELECT number, number FROM numbers(100000)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_prog SELECT number, number FROM numbers(1000)"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_mut_prog UPDATE v = v + 1 WHERE 1" --mutations_sync=0

    # For `ReplicatedMergeTree` the entry reaches the in-memory queue that backs `system.mutations`
    # asynchronously, so wait for it before reading the table.
    for _ in {1..300}; do
        mutation_id=$($CLICKHOUSE_CLIENT -q "SELECT mutation_id FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_prog' ORDER BY mutation_id LIMIT 1")
        [[ -n "$mutation_id" ]] && break
        sleep 0.3
    done

    $CLICKHOUSE_CLIENT -q "
        SELECT
            parts_to_do,
            bytes_to_do = (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = currentDatabase() AND table = 't_mut_prog' AND active),
            progress
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_mut_prog' AND NOT is_done"

    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_mut_prog"
    wait_for_mutation t_mut_prog "$mutation_id"

    $CLICKHOUSE_CLIENT -q "
        SELECT is_done, bytes_to_do, progress
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_mut_prog'"

    $CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_prog SYNC"
done
