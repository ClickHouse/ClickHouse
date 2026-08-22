#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-ordinary-database, no-fasttest
# no-fasttest: needs ZooKeeper, SYSTEM ENABLE FAILPOINT, and text_log at debug level

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_DATABASE}"
ZK="/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB} SYNC"

#### 1 - A CREATE DATABASE whose replica-node transaction commits but loses its response is completed by a retry

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_replicated_create_replica_nodes_lose_response"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
    -q "CREATE DATABASE ${DB} ENGINE=Replicated('${ZK}/ok', 's1', 'r1')"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name='${DB}'"

# Mechanism assertion, independent of the logger configuration: the reuse branch adopted the nodes the
# lost transaction had committed, so there is exactly one replica and it kept its max_log_ptr_at_creation
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT count() FROM system.zookeeper WHERE path='${ZK}/ok/replicas'"
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT count() FROM system.zookeeper WHERE path='${ZK}/ok/replicas/s1|r1' AND name='max_log_ptr_at_creation'"

# Same assertion through the log, which names the branch explicitly (needs text_log at debug level)
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.text_log WHERE logger_name='DatabaseReplicated (${DB})' AND message LIKE '%reusing them%'"

# The database is fully usable afterwards
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "CREATE TABLE ${DB}.t (k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database='${DB}'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC"

#### 2 - keeper_max_retries=0 selects the single-attempt path, so the failure is reported

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_replicated_create_replica_nodes_lose_response"

${CLICKHOUSE_CLIENT} --keeper_max_retries=0 --distributed_ddl_output_mode=none \
    -q "CREATE DATABASE ${DB} ENGINE=Replicated('${ZK}/zero', 's1', 'r1')" 2>&1 | grep -cm1 "Fault injected"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name='${DB}'"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_replicated_create_replica_nodes_lose_response"

#### 3 - A registration belonging to a different database is still refused, and the message names the recovery command

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_a SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_b SYNC"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_a ENGINE=Replicated('${ZK}/shared', 's1', 'r1')"

${CLICKHOUSE_CLIENT} \
    -q "CREATE DATABASE ${DB}_b ENGINE=Replicated('${ZK}/shared', 's1', 'r1')" 2>&1 | grep -cm1 "SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 's1' FROM ZKPATH"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_a SYNC"

#### 4 - ATTACH does not retain the creating query's process list element

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_attach SYNC"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_attach ENGINE=Replicated('${ZK}/attach', 's1', 'r1')"
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${DB}_attach"

# Both values are read by a running query, so they are equal unless the attached database kept one alive
BEFORE=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.metrics WHERE metric='Query'")
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${DB}_attach"
${CLICKHOUSE_CLIENT} -q "SELECT value = ${BEFORE} FROM system.metrics WHERE metric='Query'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_attach SYNC"

#### 5 - The recovery hint names the auxiliary Keeper, which is the one SYSTEM DROP DATABASE REPLICA acts on

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_aux SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_aux_2 SYNC"

AUX="zookeeper2:${ZK}/aux"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_aux ENGINE=Replicated('${AUX}', 's1', 'r1')"

${CLICKHOUSE_CLIENT} \
    -q "CREATE DATABASE ${DB}_aux_2 ENGINE=Replicated('${AUX}', 's1', 'r1')" 2>&1 | grep -cm1 "FROM ZKPATH '${AUX}'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_aux SYNC"
