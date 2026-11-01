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
    # The mutation reaches the queue asynchronously, so wait for a read that sizes both parts while
    # merges (and so mutations) are stopped.
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_older_insert"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_older_insert UPDATE v = v + throwIf(v = 2) WHERE 1 SETTINGS mutations_sync = 0"
    pending=""
    for _ in {1..300}; do
        pending=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_older_insert' AND parts_to_do = 2")
        [ "$pending" = "1" ] && break
        sleep 0.2
    done
    [ "$pending" = "1" ] || echo "$case FAIL: the mutation never reached the queue with both parts"
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_older_insert"

    failed=""
    for _ in {1..300}; do
        failed=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations
            WHERE database = currentDatabase() AND table = 't_older_insert' AND parts_to_do = 1 AND latest_fail_reason != ''")
        [ "$failed" = "1" ] && break
        sleep 0.2
    done
    [ "$failed" = "1" ] || echo "$case FAIL: the mutation never reached one failing part"

    # The failing part is retried, and an attempt counts the rows it has read as done until its row
    # leaves `system.merges` after it fails, so `progress` can read close to 1 meanwhile. Stop the
    # retries and wait for the last attempt to go: a newer one cannot report progress once merges stop.
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_older_insert"
    merging=""
    for _ in {1..300}; do
        merging=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_older_insert'")
        [ "$merging" = "0" ] && break
        sleep 0.2
    done
    [ "$merging" = "0" ] || echo "$case FAIL: the failing part was still being rewritten"

    state=$($CLICKHOUSE_CLIENT -q "SELECT parts_to_do, progress IS NULL, progress BETWEEN 0.3 AND 0.7 FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_older_insert' AND parts_to_do = 1 AND latest_fail_reason != ''")
    echo "$case ${state:-FAIL: the failing part is no longer the only one to do}"

    if [ "$case" = "held" ]; then
        # The block lock goes away while this replica cannot pull the replication log. A read that
        # cannot bring its queue up to date does not know whether the INSERT published a part, so
        # `progress` stays NULL and is not remembered as free of older INSERTs: it is known again
        # once pulling resumes. 05455 covers a real INSERT that publishes a part this replica has
        # not pulled yet.
        $CLICKHOUSE_CLIENT -q "SYSTEM STOP PULLING REPLICATION LOG t_older_insert"
        $CLICKHOUSE_KEEPER_CLIENT -q "rm '$zk_path/block_numbers/0/block-0000000000'"

        state_unpulled=$($CLICKHOUSE_CLIENT -q "SELECT parts_to_do, progress IS NULL, progress BETWEEN 0.3 AND 0.7 FROM system.mutations
            WHERE database = currentDatabase() AND table = 't_older_insert' AND parts_to_do = 1 AND latest_fail_reason != ''")
        echo "unpulled ${state_unpulled:-FAIL: unpulled state missing}"

        $CLICKHOUSE_CLIENT -q "SYSTEM START PULLING REPLICATION LOG t_older_insert"

        state_resumed=""
        for _ in {1..300}; do
            state_resumed=$($CLICKHOUSE_CLIENT -q "SELECT parts_to_do, progress IS NULL, progress BETWEEN 0.3 AND 0.7 FROM system.mutations
                WHERE database = currentDatabase() AND table = 't_older_insert' AND parts_to_do = 1 AND latest_fail_reason != ''")
            [ "$state_resumed" = "1	0	1" ] && break
            sleep 0.2
        done
        echo "resumed ${state_resumed:-FAIL: resumed state missing}"
    fi

    $CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_older_insert' SYNC FORMAT Null"
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_older_insert SYNC"
done
