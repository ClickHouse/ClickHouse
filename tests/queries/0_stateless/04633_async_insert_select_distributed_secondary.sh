#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
#   no-fasttest: needs a cluster and user_files for the fileCluster source.
#   zookeeper: the destination is a ReplicatedMergeTree.

# A worker-side `parallel_distributed_insert_select` insert arrives forwarded from the initiator as a
# SECONDARY_QUERY. `distributedWriteIntoReplicatedMergeTreeOrDataLakeFromClusterStorage` bails out on the
# worker (it only forwards from an INITIAL_QUERY), so the worker runs a plain, local
# `INSERT INTO replicated_dst SELECT ... FROM fileCluster(...)`. That local insert must stay synchronous:
# `is_initial_insert` is derived from the query provenance, so a SECONDARY_QUERY is never handed to the
# async insert queue. Otherwise, with `wait_for_async_insert = 0`, a shard would return after merely
# queueing its block, before the shard-local insert finished, breaking the distributed contract.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
DATA_FILE="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/data.csv"
${CLICKHOUSE_CLIENT} -q "SELECT number FROM numbers(100) FORMAT CSV" > "${DATA_FILE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_dist_secondary_dst"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_04633_dist_secondary_dst (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst', '1')
    ORDER BY x
"

# The initiator query takes the cluster-storage distributed path and forwards a local INSERT ... SELECT to
# each shard as SECONDARY_QUERY. The async settings are enabled so that, without the provenance gate, the
# worker inserts would take the queue and `wait_for_async_insert = 0` would let them return early.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04633_dist_secondary_dst
    SELECT x FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/data.csv', 'CSV', 'x UInt64')
    SETTINGS parallel_distributed_insert_select = 2,
             async_insert = 1, wait_for_async_insert = 0,
             async_insert_select_as_async_insert = 1
"

# The rows are present synchronously, right after the initiator query returned.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_dist_secondary_dst"

# No shard put its block through the async insert queue.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND table = 'test_04633_dist_secondary_dst'
"

rm -f "${DATA_FILE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_dist_secondary_dst"
