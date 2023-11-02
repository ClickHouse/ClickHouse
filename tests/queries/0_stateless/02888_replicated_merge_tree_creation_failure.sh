#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh



${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_exception_replicated SYNC"

#### 1 - There is only one replica

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -c "Fault injected"

# We will see that the replica is empty and throw the same 'Fault injected' exception as before
${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -c "Fault injected"

# We will succeed
${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_exception_replicated (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_exception_replicated SYNC"

#### 2 - There are two replicas

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -c "Fault injected"
${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated_2 (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r2') ORDER BY date" 2>&1 | grep -c "Fault injected"

# We will succeed
${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_exception_replicated (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_exception_replicated SYNC"