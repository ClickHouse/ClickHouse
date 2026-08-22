#!/usr/bin/env bash
# Tags: zookeeper

# Suppress server Error logs forwarded to client stderr (the DDL worker for replica2
# may transiently log KEEPER_EXCEPTION during initialization when databases are
# rapidly created and dropped in the retry loop).
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that replication_lag metric in system.clusters works for Replicated databases.
#
# The initiator replica (replica1) is guaranteed to have zero lag after executing a DDL,
# because tryEnqueueAndExecuteEntry commits the entry and updates log_ptr before returning.
#
# The other replica (replica2) may or may not have caught up by the time we query
# (timing-dependent), so we retry in a loop to observe lag >= 2 at least once.

ZK_PATH="/test/test_replication_lag_metric/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Retry with fresh databases each time, because once replica2 catches up, lag stays at 0.
# Use unique database names per iteration to avoid DDL worker races during rapid create/drop.
observed_lag=0
for i in $(seq 1 30); do
    DB1="rdb1_${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}"
    DB2="rdb2_${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}"

    $CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB1} ENGINE = Replicated('${ZK_PATH}/${i}', 'shard1', 'replica1')"
    $CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB2} ENGINE = Replicated('${ZK_PATH}/${i}', 'shard1', 'replica2')"

    $CLICKHOUSE_CLIENT --distributed_ddl_task_timeout 0 --query \
        "CREATE TABLE ${DB1}.t (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id"

    lag1=$($CLICKHOUSE_CLIENT --query "SELECT replication_lag FROM system.clusters WHERE cluster = '${DB1}' AND replica_num = 1")
    lag2=$($CLICKHOUSE_CLIENT --query "SELECT replication_lag FROM system.clusters WHERE cluster = '${DB1}' AND replica_num = 2")

    $CLICKHOUSE_CLIENT --query "DROP DATABASE ${DB1}"
    $CLICKHOUSE_CLIENT --query "DROP DATABASE ${DB2}"

    if [[ "$lag1" != "0" ]]; then
        echo "FAIL: initiator replica1 should have lag = 0, got $lag1"
        exit 1
    fi

    if [[ "$lag2" -ge 2 ]]; then
        observed_lag=1
        break
    fi
done

if [[ "$observed_lag" == "1" ]]; then
    echo "OK"
else
    echo "FAIL: could not observe replication_lag >= 2 on non-initiator in 30 attempts (last: replica1=$lag1 replica2=$lag2)"
    exit 1
fi
