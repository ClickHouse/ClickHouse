#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-ordinary-database, no-fasttest
# no-fasttest: needs ZooKeeper, SYSTEM ENABLE FAILPOINT, and text_log at debug level

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="rdb_${CLICKHOUSE_DATABASE}"
ZK="/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"

# A statement whose failure is the assertion keeps its own client, because its stderr is piped to
# grep. The rest are grouped so that one client runs a whole arm.

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${DB} SYNC;
    DROP DATABASE IF EXISTS ${DB}_a SYNC;
    DROP DATABASE IF EXISTS ${DB}_b SYNC;
    DROP DATABASE IF EXISTS ${DB}_attach SYNC;
    DROP DATABASE IF EXISTS ${DB}_aux SYNC;
    DROP DATABASE IF EXISTS ${DB}_aux_2 SYNC;
    DROP DATABASE IF EXISTS ${DB}_txn SYNC;
    DROP DATABASE IF EXISTS ${DB}_retry SYNC;
    DROP DATABASE IF EXISTS ${DB}_nopub SYNC;
    DROP DATABASE IF EXISTS ${DB}_reject SYNC;
"

#### 1 - A CREATE DATABASE whose replica-node transaction commits but loses its response is completed by a retry

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    SYSTEM ENABLE FAILPOINT database_replicated_create_replica_nodes_lose_response;
    CREATE DATABASE ${DB} ENGINE=Replicated('${ZK}/ok', 's1', 'r1');
    SELECT count() FROM system.databases WHERE name='${DB}';
"

# Mechanism assertion, independent of the logger configuration: the reuse branch adopted the nodes the
# lost transaction had committed, so there is exactly one replica and it kept its max_log_ptr_at_creation
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 -q "
    SELECT count() FROM system.zookeeper WHERE path='${ZK}/ok/replicas';
    SELECT count() FROM system.zookeeper WHERE path='${ZK}/ok/replicas/s1|r1' AND name='max_log_ptr_at_creation';
"

# Same assertion through the log, which names the branch explicitly (needs text_log at debug level).
# Then the database is shown to be fully usable.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    SYSTEM FLUSH LOGS text_log;
    SELECT count() > 0 FROM system.text_log WHERE logger_name='DatabaseReplicated (${DB})' AND message LIKE '%reusing them%';
    CREATE TABLE ${DB}.t (k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k;
    SELECT count() FROM system.tables WHERE database='${DB}';
    DROP DATABASE ${DB} SYNC;
"

#### 2 - keeper_max_retries=0 selects the single-attempt path, so the failure is reported

${CLICKHOUSE_CLIENT} --keeper_max_retries=0 --distributed_ddl_output_mode=none -q "
    SYSTEM ENABLE FAILPOINT database_replicated_create_replica_nodes_lose_response;
    CREATE DATABASE ${DB} ENGINE=Replicated('${ZK}/zero', 's1', 'r1');
" 2>&1 | grep -cm1 "Fault injected"

${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.databases WHERE name='${DB}';
    SYSTEM DISABLE FAILPOINT database_replicated_create_replica_nodes_lose_response;
"

#### 3 - A registration belonging to a different database is still refused, and the message names the recovery command

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_a ENGINE=Replicated('${ZK}/shared', 's1', 'r1')"

${CLICKHOUSE_CLIENT} \
    -q "CREATE DATABASE ${DB}_b ENGINE=Replicated('${ZK}/shared', 's1', 'r1')" 2>&1 | grep -cm1 "SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 's1' FROM ZKPATH"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_a SYNC"

#### 4 - ATTACH does not retain the creating query's process list element

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${DB}_attach ENGINE=Replicated('${ZK}/attach', 's1', 'r1');
    DETACH DATABASE ${DB}_attach;
"

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

AUX="zookeeper2:${ZK}/aux"

# A Keeper client whose configured chroot is absent throws at construction, and only some test
# flavors create this one, so make the root before the first database on it. Other flavors do not
# configure `zookeeper2` at all, and there `CREATE DATABASE` fails with `Unknown auxiliary ZooKeeper
# name` before reaching the code under test - print the line the arm would have produced and skip it
# rather than reporting the setup error as a result.
if ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO system.zookeeper (name, path, value) VALUES ('auxiliary_zookeeper2', '/test/chroot', '');
    CREATE DATABASE ${DB}_aux ENGINE=Replicated('${AUX}', 's1', 'r1');
" 2>/dev/null
then
    ${CLICKHOUSE_CLIENT} \
        -q "CREATE DATABASE ${DB}_aux_2 ENGINE=Replicated('${AUX}', 's1', 'r1')" 2>&1 | grep -cm1 "FROM ZKPATH '${AUX}'"

    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_aux SYNC"
else
    echo 1
fi

#### 6 - A CREATE TABLE carried by a DDL log entry keeps Keeper intact when it fails after the commit

# The entry's metadata transaction is committed before the table reaches the database, so the table is
# absent locally while the entry is already visible to the other replicas. Removing the replica here
# would delete a table subtree those replicas are about to use.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_txn ENGINE=Replicated('${ZK}/txn', 's1', 'r1')"

TBL_ZK="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/txn_commit"

# The digest starts at 0 and the entry's transaction is what advances it, so a non-zero value below is
# what proves this arm really runs after the commit rather than before it
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 -q "
    SELECT value FROM system.zookeeper WHERE path='${ZK}/txn/replicas/s1|r1' AND name='digest';
    SYSTEM ENABLE FAILPOINT database_atomic_fail_after_committing_metadata_transaction;
"

${CLICKHOUSE_CLIENT} --database_replicated_allow_replicated_engine_arguments=3 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_txn.t (k UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/txn_commit/{shard}', '{replica}') ORDER BY k" 2>&1 | grep -cm1 "Fault injected (after committing metadata"

# Keeper keeps what the committed entry published: the entry itself, and the table subtree with this
# replica's registration, which the other replicas are about to use
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 -q "
    SELECT count() FROM system.zookeeper WHERE path='${ZK}/txn/metadata' AND name='t';
    SELECT name FROM system.zookeeper WHERE path='${TBL_ZK}/s1/replicas';
    SELECT count() FROM system.zookeeper WHERE path='${TBL_ZK}/s1' AND name='metadata';
    SELECT value != '0' FROM system.zookeeper WHERE path='${ZK}/txn/replicas/s1|r1' AND name='digest';
    DROP DATABASE ${DB}_txn SYNC;
"

#### 7 - The local data directory is still cleaned up when the registration has to be kept

# Keeping the registration must not also keep the local directory. With an explicit UUID the retry
# resolves to the same data path, so a leftover directory fails it with TABLE_ALREADY_EXISTS.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_retry ENGINE=Replicated('${ZK}/retry', 's1', 'r1')"

RETRY_UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT reinterpretAsUUID('${CLICKHOUSE_DATABASE}retry')")

${CLICKHOUSE_CLIENT} --database_replicated_allow_explicit_uuid 3 \
    --create_replicated_merge_tree_fault_injection_probability=1 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_retry.t UUID '${RETRY_UUID}' (k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k" 2>&1 | grep -cm1 "Fault injected"

# The retry reuses both the registration and the freed data path, so the table ends up usable
${CLICKHOUSE_CLIENT} --database_replicated_allow_explicit_uuid 3 --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${DB}_retry.t UUID '${RETRY_UUID}' (k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k;
    INSERT INTO ${DB}_retry.t SELECT 1;
    SELECT count() FROM ${DB}_retry.t;
    DROP DATABASE ${DB}_retry SYNC;
"

#### 8 - A CREATE TABLE carried by a DDL log entry cleans Keeper up when it fails before the commit

# The entry's metadata transaction is what publishes it, and it is committed inside the step this
# failpoint precedes, so nothing here is visible to the other replicas and the registration is this
# statement's to remove. With an implicit UUID the retry gets a fresh one, so a leftover registration
# no longer matches its identity and cannot be reused: the retry is what pins the removal.
NOPUB_ZK="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/nopub_table"

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${DB}_nopub ENGINE=Replicated('${ZK}/nopub', 's1', 'r1');
    SYSTEM ENABLE FAILPOINT database_on_disk_fail_before_commit_create_table;
"

${CLICKHOUSE_CLIENT} --database_replicated_allow_replicated_engine_arguments=3 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_nopub.t (k UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nopub_table/{shard}', '{replica}') ORDER BY k" 2>&1 | grep -cm1 "Fault injected (before"

# Nothing was published, which is the premise of the arm, and the registration is gone
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 -q "
    SELECT count() FROM system.zookeeper WHERE path='${ZK}/nopub/metadata' AND name='t';
    SELECT count() FROM system.zookeeper WHERE path='${NOPUB_ZK}/s1/replicas';
"

# So a plain retry, which regenerates the UUID, completes and the table is usable
${CLICKHOUSE_CLIENT} --database_replicated_allow_replicated_engine_arguments=3 --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${DB}_nopub.t (k UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nopub_table/{shard}', '{replica}') ORDER BY k;
    INSERT INTO ${DB}_nopub.t SELECT 1;
    SELECT count() FROM ${DB}_nopub.t;
    DROP DATABASE ${DB}_nopub SYNC;
"

#### 9 - A CREATE TABLE whose entry transaction is rejected atomically cleans Keeper up

# The entry transaction of an initial query creates the metadata node of the new table, so a
# pre-existing node there makes the whole transaction fail atomically. It writes nothing, so the entry
# never gets its `/committed` and no other replica can execute it, which makes the registration this
# statement left behind its own to remove.
REJECT_ZK="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/reject_table"

# The node planted below is not valid table metadata, and a replica that reads it during its
# startup snapshot of `/metadata` cannot parse it and retries startup forever, so the sync is what
# puts the replica past that read.
# The last SELECT is the arming condition, asserted separately from the oracle below
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 -q "
    CREATE DATABASE ${DB}_reject ENGINE=Replicated('${ZK}/reject', 's1', 'r1');
    SYSTEM SYNC DATABASE REPLICA ${DB}_reject;
    INSERT INTO system.zookeeper (name, path, value) VALUES ('t', '${ZK}/reject/metadata', 'blocks the entry transaction');
    SELECT count() FROM system.zookeeper WHERE path='${ZK}/reject/metadata' AND name='t';
"

${CLICKHOUSE_CLIENT} --database_replicated_allow_replicated_engine_arguments=3 --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_reject.t (k UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/reject_table/{shard}', '{replica}') ORDER BY k" 2>&1 | grep -cm1 "Transaction failed (Node exists)"

# Premise: the entry is unpublished, so no other replica will ever execute it
REJECT_ENTRY=$(${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 \
    -q "SELECT max(name) FROM system.zookeeper WHERE path='${ZK}/reject/log'")
# The third SELECT is the oracle: the registration this statement created is gone
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 -q "
    SELECT count() FROM system.zookeeper WHERE path='${ZK}/reject/log/${REJECT_ENTRY}' AND name='committed';
    SELECT count() FROM system.tables WHERE database='${DB}_reject' AND name='t';
    SELECT count() FROM system.zookeeper WHERE path='${REJECT_ZK}/s1/replicas';
"

# Which is what the removal buys: the same table Keeper path is reusable, whereas a leftover
# registration of an implicit UUID matches no later statement and could never be reused
${CLICKHOUSE_CLIENT} --database_replicated_allow_replicated_engine_arguments=3 --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${DB}_reject.t2 (k UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/reject_table/{shard}', '{replica}') ORDER BY k;
    INSERT INTO ${DB}_reject.t2 SELECT 1;
    SELECT count() FROM ${DB}_reject.t2;
    DROP DATABASE ${DB}_reject SYNC;
"

# No failpoint may leak into a later run of this test
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.fail_points WHERE enabled AND name IN ('database_replicated_create_replica_nodes_lose_response', 'database_atomic_fail_after_committing_metadata_transaction', 'database_on_disk_fail_before_commit_create_table')"
