#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression for https://github.com/ClickHouse/ClickHouse/issues/114678: a replicated mutation lists
# the parts of log entries this replica has not executed yet, so a part still to be fetched has no
# size here. `progress` must stay unset in that state rather than measure only the work it can see,
# while `bytes_to_do` keeps the size of the remaining parts that are on disk.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_unknown_1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mut_unknown_2 SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_unknown_1 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_unknown', '1') ORDER BY k"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut_unknown_2 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_unknown', '2') ORDER BY k"

# The second replica gets the first part and then stops fetching, so the mutation's scope there is
# one part on disk and one part that only exists in the queue. Merges are stopped on it as well, so
# nothing is rewritten and the numbers below are exact.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_unknown_1 SELECT number, number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_mut_unknown_2"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP FETCHES t_mut_unknown_2"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_mut_unknown_2"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut_unknown_1 SELECT number, number FROM numbers(100000)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mut_unknown_1 UPDATE v = v + 1 WHERE 1" --mutations_sync=0

# The entry reaches the second replica's in-memory queue, which backs `system.mutations`, asynchronously.
for _ in {1..300}; do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_unknown_2'")" -ge 1 ]]; then
        break
    fi
    sleep 0.3
done

$CLICKHOUSE_CLIENT -q "
    SELECT
        parts_to_do,
        bytes_to_do > 0,
        bytes_to_do = (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = currentDatabase() AND table = 't_mut_unknown_2' AND active),
        progress IS NULL
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_mut_unknown_2'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_unknown_1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_mut_unknown_2 SYNC"
