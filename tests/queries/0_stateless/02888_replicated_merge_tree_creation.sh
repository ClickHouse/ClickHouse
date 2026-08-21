#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --database_replicated_allow_explicit_uuid 3 --database_replicated_allow_replicated_engine_arguments 3"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_exception_replicated SYNC"

UUID=$(${CLICKHOUSE_CLIENT} --query "SELECT reinterpretAsUUID(currentDatabase())")

#### 1 - There is only one replica

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# We will see that the replica is empty and throw the same 'Fault injected' exception as before
${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# We will succeed
${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_exception_replicated SYNC"

#### 2 - There are two replicas

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"
${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated_2 (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r2') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# We will succeed
${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date"

# The second replica cleans up its own registration now, so nothing is left to drop separately
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate/replicas' ORDER BY name"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_exception_replicated SYNC"

#### 3 - A CREATE without an explicit UUID (the case reported in issue #69433) can be retried

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_no_uuid SYNC"

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_no_uuid (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/no_uuid', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# Nothing is left behind in Keeper, so the next CREATE (with a fresh table UUID) succeeds
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/no_uuid/replicas'"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_no_uuid (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/no_uuid', 'r1') ORDER BY date"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='test_no_uuid'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_no_uuid SYNC"

#### 4 - A failing replica leaves the other replicas and the shared table path untouched

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_keep_first SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_keep_second SYNC"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_keep_first (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep', 'r1') ORDER BY date"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_keep_first SELECT toDate('2024-01-01')"

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_keep_second (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep', 'r2') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# Only the failing replica is gone; the shared path and the live replica are intact and still writable
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep/replicas' ORDER BY name"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep' AND name='metadata'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_keep_first SELECT toDate('2024-01-02')"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_keep_first"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_keep_second (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep', 'r2') ORDER BY date"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_keep_second SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_keep_first SYNC"

#### 5 - An ATTACH failure must not remove Keeper state

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_attach SYNC"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_attach (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/attach', 'r1') ORDER BY date"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE test_attach"

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "ATTACH TABLE test_attach" 2>&1 | grep -cm1 "Fault injected"

# The registration is still there, so the table can be attached again
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/attach/replicas' ORDER BY name"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE test_attach"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='test_attach'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_attach SYNC"

#### 6 - The REPLICA_ALREADY_EXISTS message names the recovery command

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_hint SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_hint_2 SYNC"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_hint (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/hint', 'r1') ORDER BY date"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_hint_2 (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/hint', 'r1') ORDER BY date" 2>&1 | grep -cm1 "SYSTEM DROP REPLICA 'r1' FROM ZKPATH"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_hint SYNC"
