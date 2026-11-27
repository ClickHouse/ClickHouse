#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree
# Tag no-parallel: SYSTEM RECONNECT ZOOKEEPER expires the Keeper session of every table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation killed by another replica while r1 rebuilds its queue never reaches the usual
# removal paths, so the rebuild itself must release the progress state r1 keeps for it.

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS r1 SYNC;
    DROP TABLE IF EXISTS r2 SYNC;
    CREATE TABLE r1 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t', 'r1')
        ORDER BY k SETTINGS zookeeper_session_expiration_check_period = 1;
    CREATE TABLE r2 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t', 'r2')
        ORDER BY k SETTINGS zookeeper_session_expiration_check_period = 1;
    SYSTEM STOP MERGES r1;
    SYSTEM STOP MERGES r2;
    INSERT INTO r1 SELECT number, number FROM numbers(1000);
    SYSTEM SYNC REPLICA r2;
    ALTER TABLE r1 UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 0;
    ALTER TABLE r1 UPDATE v = v + 2 WHERE 1 SETTINGS mutations_sync = 0;
"

function wait_for()
{
    for _ in {1..300}; do
        [[ $($CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log; $1") == 1 ]] && return
        sleep 0.2
    done
    echo "Timed out: $1"
}

function mutations_are()
{
    echo "SELECT count() = $2 FROM system.mutations WHERE database = currentDatabase() AND table = '$1'"
}

# Reading r1 sizes both mutations, which creates their progress state.
wait_for "$(mutations_are r1 2)"
wait_for "$(mutations_are r2 2)"
$CLICKHOUSE_CLIENT -q "SELECT mutation_id FROM system.mutations WHERE database = currentDatabase() AND table = 'r1' ORDER BY mutation_id"

# r1 must not learn about the kill before its queue is rebuilt.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP PULLING REPLICATION LOG r1"
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 'r2' AND mutation_id = '0000000001' FORMAT Null"

start_time=$($CLICKHOUSE_CLIENT -q "SELECT now64(6)")
$CLICKHOUSE_CLIENT -q "SYSTEM RECONNECT ZOOKEEPER"

queue_log="SELECT count() > 0 FROM system.text_log WHERE event_time_microseconds >= '$start_time'
    AND logger_name = currentDatabase() || '.r1 (ReplicatedMergeTreeQueue)' AND message LIKE"

# The rebuild cleared the queue, and with pulling stopped it cannot load the mutations yet.
wait_for "$queue_log 'Queue initialized'"
$CLICKHOUSE_CLIENT -q "SYSTEM START PULLING REPLICATION LOG r1"

wait_for "$queue_log 'Mutation 0000000001 was removed while the queue was rebuilt%'"
wait_for "$(mutations_are r1 1)"
$CLICKHOUSE_CLIENT -q "$queue_log 'Mutation 0000000000 was removed%'"

# The surviving mutation keeps its progress.
$CLICKHOUSE_CLIENT -q "SELECT mutation_id, progress FROM system.mutations WHERE database = currentDatabase() AND table = 'r1' ORDER BY mutation_id"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE r1 SYNC;
    DROP TABLE r2 SYNC;
"
