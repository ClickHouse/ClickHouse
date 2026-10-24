#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An older INSERT that has not published its part yet keeps `progress` NULL even with a known part left.
# A persistent `block-0000000000` node under block_numbers stands in for that INSERT.

for case in control held; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_older_insert SYNC"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_older_insert (p UInt8, k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_older_insert_$case', 'r1') PARTITION BY p ORDER BY k"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_older_insert SELECT 0, number, 1 FROM numbers(1000)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_older_insert SELECT 1, number, 2 FROM numbers(1000)"

    if [ "$case" = "held" ]; then
        zk_path=$($CLICKHOUSE_CLIENT -q "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 't_older_insert'")
        $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('block-0000000000', '$zk_path/block_numbers/0', '')"
    fi

    # The part of partition 1 fails forever, so one part with a known size stays to do.
    # The first read sizes both parts while merges (and so mutations) are stopped.
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_older_insert"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_older_insert UPDATE v = v + throwIf(v = 2) WHERE 1 SETTINGS mutations_sync = 0"
    $CLICKHOUSE_CLIENT -q "SELECT parts_to_do FROM system.mutations WHERE database = currentDatabase() AND table = 't_older_insert' FORMAT Null"
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_older_insert"

    state=""
    for _ in {1..300}; do
        state=$($CLICKHOUSE_CLIENT -q "SELECT parts_to_do, progress IS NULL, progress BETWEEN 0.3 AND 0.7 FROM system.mutations
            WHERE database = currentDatabase() AND table = 't_older_insert' AND parts_to_do = 1 AND latest_fail_reason != ''")
        [ -n "$state" ] && break
        sleep 0.2
    done
    echo "$case ${state:-FAIL: the mutation never reached one failing part}"

    $CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_older_insert' SYNC FORMAT Null"
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_older_insert SYNC"
done
