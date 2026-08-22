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

# Both values are read by a running query, so they are equal unless the attached database kept one
# alive. The gauge counts every process-list query, internal ones included, so an unrelated query can
# be in flight here; an increment the attached database retains never goes away.
BEFORE=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.metrics WHERE metric='Query'")
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${DB}_attach"
BACK=0
for _ in {1..60}; do
    if [ "$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.metrics WHERE metric='Query'")" = "${BEFORE}" ]; then
        BACK=1
        break
    fi
    sleep 0.5
done
echo "${BACK}"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_attach SYNC"

#### 5 - The recovery hint names the auxiliary Keeper, which is the one SYSTEM DROP DATABASE REPLICA acts on

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_aux SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_aux_2 SYNC"

# A Keeper client whose configured chroot is absent throws at construction, and only some test
# flavors create this one, so make the root before the first database on it.
${CLICKHOUSE_CLIENT} -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('auxiliary_zookeeper2', '/test/chroot', '')"

AUX="zookeeper2:${ZK}/aux"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_aux ENGINE=Replicated('${AUX}', 's1', 'r1')"

${CLICKHOUSE_CLIENT} \
    -q "CREATE DATABASE ${DB}_aux_2 ENGINE=Replicated('${AUX}', 's1', 'r1')" 2>&1 | grep -cm1 "FROM ZKPATH '${AUX}'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_aux SYNC"

#### 6 - A CREATE TABLE carried by a DDL log entry keeps Keeper intact when it fails after the commit

# The entry's metadata transaction is committed before the table reaches the database, so the table is
# absent locally while the entry is already visible to the other replicas. Removing the replica here
# would delete a table subtree those replicas are about to use.
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_txn SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_txn ENGINE=Replicated('${ZK}/txn', 's1', 'r1')"

TBL_ZK="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/txn_commit"

# The digest starts at 0 and the entry's transaction is what advances it, so a non-zero value below is
# what proves this arm really runs after the commit rather than before it
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT value FROM system.zookeeper WHERE path='${ZK}/txn/replicas/s1|r1' AND name='digest'"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_atomic_fail_after_committing_metadata_transaction"

${CLICKHOUSE_CLIENT} --database_replicated_allow_replicated_engine_arguments=3 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_txn.t (k UInt64) ENGINE=ReplicatedMergeTree('${TBL_ZK}/{shard}', '{replica}') ORDER BY k" 2>&1 | grep -cm1 "Fault injected (after committing metadata"

# Keeper keeps what the committed entry published: the entry itself, and the table subtree with this
# replica's registration, which the other replicas are about to use
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT count() FROM system.zookeeper WHERE path='${ZK}/txn/metadata' AND name='t'"
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT name FROM system.zookeeper WHERE path='${TBL_ZK}/s1/replicas'"
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT count() FROM system.zookeeper WHERE path='${TBL_ZK}/s1' AND name='metadata'"
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT value != '0' FROM system.zookeeper WHERE path='${ZK}/txn/replicas/s1|r1' AND name='digest'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_txn SYNC"

#### 7 - The local data directory is still cleaned up when the registration has to be kept

# Keeping the registration must not also keep the local directory. With an explicit UUID the retry
# resolves to the same data path, so a leftover directory fails it with TABLE_ALREADY_EXISTS.
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_retry SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_retry ENGINE=Replicated('${ZK}/retry', 's1', 'r1')"

RETRY_UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT reinterpretAsUUID('${CLICKHOUSE_DATABASE}retry')")

${CLICKHOUSE_CLIENT} --database_replicated_allow_explicit_uuid 3 \
    --create_replicated_merge_tree_fault_injection_probability=1 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_retry.t UUID '${RETRY_UUID}' (k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k" 2>&1 | grep -cm1 "Fault injected"

# The retry reuses both the registration and the freed data path, so the table ends up usable
${CLICKHOUSE_CLIENT} --database_replicated_allow_explicit_uuid 3 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_retry.t UUID '${RETRY_UUID}' (k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}_retry.t SELECT 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}_retry.t"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_retry SYNC"

#### 8 - A CREATE TABLE carried by a DDL log entry cleans Keeper up when it fails before the commit

# The entry's metadata transaction is what publishes it, and it is committed inside the step this
# failpoint precedes, so nothing here is visible to the other replicas and the registration is this
# statement's to remove. With an implicit UUID the retry gets a fresh one, so a leftover registration
# no longer matches its identity and cannot be reused: the retry is what pins the removal.
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_nopub SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_nopub ENGINE=Replicated('${ZK}/nopub', 's1', 'r1')"

NOPUB_ZK="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/nopub_table"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_on_disk_fail_before_commit_create_table"

${CLICKHOUSE_CLIENT} --database_replicated_allow_replicated_engine_arguments=3 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_nopub.t (k UInt64) ENGINE=ReplicatedMergeTree('${NOPUB_ZK}/{shard}', '{replica}') ORDER BY k" 2>&1 | grep -cm1 "Fault injected (before"

# Nothing was published, which is the premise of the arm, and the registration is gone
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT count() FROM system.zookeeper WHERE path='${ZK}/nopub/metadata' AND name='t'"
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT count() FROM system.zookeeper WHERE path='${NOPUB_ZK}/s1/replicas'"

# So a plain retry, which regenerates the UUID, completes and the table is usable
${CLICKHOUSE_CLIENT} --database_replicated_allow_replicated_engine_arguments=3 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_nopub.t (k UInt64) ENGINE=ReplicatedMergeTree('${NOPUB_ZK}/{shard}', '{replica}') ORDER BY k"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}_nopub.t SELECT 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB}_nopub.t"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_nopub SYNC"

# No failpoint may leak into a later run of this test
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.fail_points WHERE enabled AND name IN ('database_replicated_create_replica_nodes_lose_response', 'database_atomic_fail_after_committing_metadata_transaction', 'database_on_disk_fail_before_commit_create_table')"
