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

#### 7 - A Keeper failure inside the constructor, with default settings, cleans up the registration

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_ctor SYNC"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT replicated_merge_tree_fail_after_creating_replica"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_ctor (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor/replicas'"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_ctor (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor', 'r1') ORDER BY date"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='test_ctor'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_ctor SYNC"

#### 8 - The same failure on a second replica keeps the first replica and the shared path

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_ctor_first SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_ctor_second SYNC"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_ctor_first (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep', 'r1') ORDER BY date"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_ctor_first SELECT toDate('2024-01-01')"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT replicated_merge_tree_fail_after_creating_replica"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_ctor_second (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep', 'r2') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep/replicas' ORDER BY name"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep' AND name='metadata'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_ctor_first SELECT toDate('2024-01-02')"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_ctor_first"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_ctor_second (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep', 'r2') ORDER BY date"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_ctor_second SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_ctor_first SYNC"

#### 9 - The recovery hint names the auxiliary Keeper, so the command it prints acts on the right one

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_aux SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_aux_2 SYNC"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_aux (date Date) ENGINE=ReplicatedMergeTree('zookeeper2:/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux', 'r1') ORDER BY date"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_aux_2 (date Date) ENGINE=ReplicatedMergeTree('zookeeper2:/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux', 'r1') ORDER BY date" 2>&1 | grep -cm1 "SYSTEM DROP REPLICA 'r1' FROM ZKPATH 'zookeeper2:/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux'"

# The path the hint prints must be the one SYSTEM DROP REPLICA resolves: dropping the same replica
# without the prefix is routed to the default Keeper, where that path does not exist.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE test_aux PERMANENTLY"
${CLICKHOUSE_CLIENT} \
    -q "SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux'" 2>&1 | grep -cm1 "does not look like a table path"
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT name FROM system.zookeeper WHERE zookeeperName='zookeeper2' AND path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux/replicas'"
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP REPLICA 'r1' FROM ZKPATH 'zookeeper2:/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux'"
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT count() FROM system.zookeeper WHERE zookeeperName='zookeeper2' AND path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux/replicas'"

${CLICKHOUSE_CLIENT} -q "ATTACH TABLE test_aux"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_aux SYNC"
