#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database
# no-parallel: uses PAUSEABLE_ONCE failpoints that fire exactly once globally; a concurrent test
#   from another parallel run could steal the failpoint pause and cause this test to hang.
# no-fasttest: requires Keeper, and the Kafka engine is not available in fast tests.
# no-replicated-database: uses an explicit `kafka_keeper_path` that conflicts with the DDL
#   replication mechanism of DatabaseReplicated.
#
# Regression test: LOGICAL_ERROR "There is a race condition between creation and removal[ of
# replicated table]" must not be thrown by StorageKafka2::removeTableNodesFromZooKeeper when a
# Keeper session expires mid-cleanup and a concurrent operation finishes removing the nodes.
#
# The same class was fixed for StorageReplicatedMergeTree by #99557 (see
# 04036_replicated_table_race_creation_removal.sh); StorageKafka2 is a copy of the same
# create/drop coordination protocol and kept both throws.
#
# Two scenarios are exercised via DROP TABLE SYNC (which calls removeTableNodesFromZooKeeper):
#   A) the path is gone before tryGetChildren    -> ZNONODE on tryGetChildren
#   B) the path is gone before the final tryMulti -> ZNONODE on tryMulti
#
# Case A returns true (the root znode is gone, so the table IS completely removed); case B
# leaves completely_removed == false (only some of the three nodes may be gone).
#
# The broker address is deliberately unreachable: StorageKafka2's constructor only creates the
# Keeper nodes, consumers are created later in startup() and a broker failure there is not fatal.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ZK_PATH="/clickhouse/kafka2/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/k2_race"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT kafka2_remove_zk_before_get_children" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT kafka2_remove_zk_before_final_multi" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS k2_race SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${ZK_PATH}'" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

function create_table()
{
    ${CLICKHOUSE_CLIENT} --allow_experimental_kafka_offsets_storage_in_keeper=1 -q "
        CREATE TABLE k2_race (a String) ENGINE = Kafka
        SETTINGS kafka_broker_list = 'localhost:1',
                 kafka_topic_list = 'k2_race_topic',
                 kafka_group_name = '${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}',
                 kafka_format = 'RawBLOB',
                 kafka_keeper_path = '${ZK_PATH}',
                 kafka_replica_name = 'r1'
    "
}

# Remove the whole Keeper subtree, emulating a concurrent operation that finished the cleanup.
# NOTE: `clickhouse keeper-client -q` always exits with 0, even when the Keeper request fails,
# so the removal MUST be verified explicitly — otherwise the race is never set up and the test
# would pass vacuously even with the fix reverted.
function remove_keeper_subtree_and_verify()
{
    ${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${ZK_PATH}'" 2>/dev/null ||:

    local still_there
    still_there=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.zookeeper
        WHERE path = '/clickhouse/kafka2/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}' AND name = 'k2_race'
    ")
    if [[ "$still_there" != "0" ]]; then
        echo "FAIL: ${ZK_PATH} is still present in Keeper, the race was not set up"
        exit 1
    fi
}

function wait_for_pause()
{
    local failpoint=$1
    if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"; then
        echo "FAIL: DROP never reached the failpoint ${failpoint}"
        exit 1
    fi
}

function wait_for_drop()
{
    local drop_pid=$1
    local scenario=$2
    if ! wait "$drop_pid"; then
        echo "FAIL: DROP TABLE failed in scenario ${scenario} (LOGICAL_ERROR thrown or server died)"
        exit 1
    fi
}

# ===== Scenario A: ZNONODE on tryGetChildren =====

create_table

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT kafka2_remove_zk_before_get_children"

# DROP TABLE SYNC triggers dropReplica -> removeTableNodesFromZooKeeper, which pauses on the
# failpoint before calling tryGetChildren.
${CLICKHOUSE_CLIENT} -q "DROP TABLE k2_race SYNC" &
DROP_PID=$!

wait_for_pause kafka2_remove_zk_before_get_children
remove_keeper_subtree_and_verify

# Resume: tryGetChildren returns ZNONODE. With the fix we log a warning and return true
# instead of throwing LOGICAL_ERROR.
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT kafka2_remove_zk_before_get_children"

wait_for_drop $DROP_PID A

echo "Scenario A passed"

# ===== Scenario B: ZNONODE on the final tryMulti =====

create_table

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT kafka2_remove_zk_before_final_multi"

${CLICKHOUSE_CLIENT} -q "DROP TABLE k2_race SYNC" &
DROP_PID=$!

wait_for_pause kafka2_remove_zk_before_final_multi
remove_keeper_subtree_and_verify

# Resume: tryMulti returns ZNONODE. With the fix we log a warning and leave
# completely_removed == false instead of throwing LOGICAL_ERROR.
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT kafka2_remove_zk_before_final_multi"

wait_for_drop $DROP_PID B

echo "Scenario B passed"
