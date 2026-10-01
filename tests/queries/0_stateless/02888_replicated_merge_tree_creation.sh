#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --database_replicated_allow_explicit_uuid 3 --database_replicated_allow_replicated_engine_arguments 3"

# A statement whose failure is the assertion keeps its own client, because its stderr is piped to
# grep. The rest are grouped so that one client runs a whole arm.

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS test_exception_replicated SYNC;
    DROP TABLE IF EXISTS test_no_uuid SYNC;
    DROP TABLE IF EXISTS test_keep_first SYNC;
    DROP TABLE IF EXISTS test_keep_second SYNC;
    DROP TABLE IF EXISTS test_attach SYNC;
    DROP TABLE IF EXISTS test_hint SYNC;
    DROP TABLE IF EXISTS test_hint_2 SYNC;
    DROP TABLE IF EXISTS test_ctor SYNC;
    DROP TABLE IF EXISTS test_ctor_first SYNC;
    DROP TABLE IF EXISTS test_ctor_second SYNC;
    DROP TABLE IF EXISTS test_aux SYNC;
    DROP TABLE IF EXISTS test_aux_2 SYNC;
    DROP TABLE IF EXISTS test_before_commit SYNC;
    DROP TABLE IF EXISTS test_after_commit SYNC;
    DROP TABLE IF EXISTS test_zero_copy SYNC;
    DROP TABLE IF EXISTS test_zero_copy_unnamed SYNC;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_nouuid SYNC;
"

UUID=$(${CLICKHOUSE_CLIENT} --query "SELECT reinterpretAsUUID(currentDatabase())")

#### 1 - There is only one replica

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# We will see that the replica is empty and throw the same 'Fault injected' exception as before
${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# We will succeed
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date;
    DROP TABLE test_exception_replicated SYNC;
"

#### 2 - There are two replicas

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"
${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_exception_replicated_2 (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r2') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# We will succeed. The second replica cleans up its own registration, so nothing is left to drop
# separately.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_exception_replicated UUID '$UUID' (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate', 'r1') ORDER BY date;
    SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/recreate/replicas' ORDER BY name;
    DROP TABLE test_exception_replicated SYNC;
"

#### 3 - A CREATE without an explicit UUID (the case reported in issue #69433) can be retried

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_no_uuid (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/no_uuid', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# Nothing is left behind in Keeper, so the next CREATE (with a fresh table UUID) succeeds
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/no_uuid/replicas';
    CREATE TABLE test_no_uuid (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/no_uuid', 'r1') ORDER BY date;
    SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='test_no_uuid';
    DROP TABLE test_no_uuid SYNC;
"

#### 4 - A failing replica leaves the other replicas and the shared table path untouched

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_keep_first (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep', 'r1') ORDER BY date;
    INSERT INTO test_keep_first SELECT toDate('2024-01-01');
"

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE test_keep_second (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep', 'r2') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

# Only the failing replica is gone; the shared path and the live replica are intact and still writable
${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep/replicas' ORDER BY name;
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep' AND name='metadata';
    INSERT INTO test_keep_first SELECT toDate('2024-01-02');
    SELECT count() FROM test_keep_first;
    CREATE TABLE test_keep_second (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/keep', 'r2') ORDER BY date;
    DROP TABLE test_keep_second SYNC;
    DROP TABLE test_keep_first SYNC;
"

#### 5 - An ATTACH failure must not remove Keeper state

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_attach (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/attach', 'r1') ORDER BY date;
    DETACH TABLE test_attach;
"

${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "ATTACH TABLE test_attach" 2>&1 | grep -cm1 "Fault injected"

# The registration is still there, so the table can be attached again
${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/attach/replicas' ORDER BY name;
    ATTACH TABLE test_attach;
    SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='test_attach';
    DROP TABLE test_attach SYNC;
"

#### 6 - The REPLICA_ALREADY_EXISTS message names the recovery command

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_hint (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/hint', 'r1') ORDER BY date"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_hint_2 (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/hint', 'r1') ORDER BY date" 2>&1 | grep -cm1 "SYSTEM DROP REPLICA 'r1' FROM ZKPATH"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_hint SYNC"

#### 7 - A Keeper failure inside the constructor, with default settings, cleans up the registration

${CLICKHOUSE_CLIENT} -q "
    SYSTEM ENABLE FAILPOINT replicated_merge_tree_fail_after_creating_replica;
    CREATE TABLE test_ctor (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor', 'r1') ORDER BY date;
" 2>&1 | grep -cm1 "Fault injected"

${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor/replicas';
    CREATE TABLE test_ctor (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor', 'r1') ORDER BY date;
    SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='test_ctor';
    DROP TABLE test_ctor SYNC;
"

#### 8 - The same failure on a second replica keeps the first replica and the shared path

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_ctor_first (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep', 'r1') ORDER BY date;
    INSERT INTO test_ctor_first SELECT toDate('2024-01-01');
    SYSTEM ENABLE FAILPOINT replicated_merge_tree_fail_after_creating_replica;
"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_ctor_second (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep', 'r2') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep/replicas' ORDER BY name;
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep' AND name='metadata';
    INSERT INTO test_ctor_first SELECT toDate('2024-01-02');
    SELECT count() FROM test_ctor_first;
    CREATE TABLE test_ctor_second (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ctor_keep', 'r2') ORDER BY date;
    DROP TABLE test_ctor_second SYNC;
    DROP TABLE test_ctor_first SYNC;
"

#### 9 - The recovery hint names the auxiliary Keeper, so the command it prints acts on the right one

# A Keeper client whose configured chroot is absent throws at construction, and only some test
# flavors create this one, so make the root before the first table on it.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO system.zookeeper (name, path, value) VALUES ('auxiliary_zookeeper2', '/test/chroot', '');
    CREATE TABLE test_aux (date Date) ENGINE=ReplicatedMergeTree('zookeeper2:/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux', 'r1') ORDER BY date;
"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_aux_2 (date Date) ENGINE=ReplicatedMergeTree('zookeeper2:/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux', 'r1') ORDER BY date" 2>&1 | grep -cm1 "SYSTEM DROP REPLICA 'r1' FROM ZKPATH 'zookeeper2:/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux'"

# The path the hint prints must be the one SYSTEM DROP REPLICA resolves: dropping the same replica
# without the prefix is routed to the default Keeper, where that path does not exist.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE test_aux PERMANENTLY"
${CLICKHOUSE_CLIENT} \
    -q "SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux'" 2>&1 | grep -cm1 "does not look like a table path"
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 -q "
    SELECT name FROM system.zookeeper WHERE zookeeperName='zookeeper2' AND path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux/replicas';
    SYSTEM DROP REPLICA 'r1' FROM ZKPATH 'zookeeper2:/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux';
    SELECT count() FROM system.zookeeper WHERE zookeeperName='zookeeper2' AND path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/aux/replicas';
    ATTACH TABLE test_aux;
    DROP TABLE test_aux SYNC;
"

#### 10 - A database whose tables have no UUID: a colliding CREATE must not touch the existing replica

# `Memory` has no database UUID, so every table in it has a nil UUID and the registrations of all of
# them carry the same identity. A failed CREATE therefore cannot prove which one is its own.
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_nouuid ENGINE=Memory;
    CREATE TABLE ${CLICKHOUSE_DATABASE}_nouuid.live (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid', 'r1') ORDER BY date;
    INSERT INTO ${CLICKHOUSE_DATABASE}_nouuid.live SELECT toDate('2024-01-01');
"

# Same Keeper path and replica name as the live table
${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_nouuid.other (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid', 'r1') ORDER BY date" 2>&1 | grep -cm1 "REPLICA_ALREADY_EXISTS"

# The same collision while the table is detached: there is no active node to fall back on, so the
# identity check is the only thing that can tell the two tables apart.
${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid/replicas' ORDER BY name;
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid' AND name='metadata';
    INSERT INTO ${CLICKHOUSE_DATABASE}_nouuid.live SELECT toDate('2024-01-02');
    SELECT count() FROM ${CLICKHOUSE_DATABASE}_nouuid.live;
    DETACH TABLE ${CLICKHOUSE_DATABASE}_nouuid.live;
"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_nouuid.other (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid', 'r1') ORDER BY date" 2>&1 | grep -cm1 "REPLICA_ALREADY_EXISTS"

${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid/replicas' ORDER BY name;
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid' AND name='metadata';
    ATTACH TABLE ${CLICKHOUSE_DATABASE}_nouuid.live;
    SELECT count() FROM ${CLICKHOUSE_DATABASE}_nouuid.live;
"

# A failure of the same statement that registered the replica also keeps it, for the same reason. The
# registration stays reusable, so a retry still completes.
${CLICKHOUSE_CLIENT} --create_replicated_merge_tree_fault_injection_probability=1 \
    -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_nouuid.own (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid_own', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected"

${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid_own/replicas';
    CREATE TABLE ${CLICKHOUSE_DATABASE}_nouuid.own_2 (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/nouuid_own', 'r1') ORDER BY date;
    INSERT INTO ${CLICKHOUSE_DATABASE}_nouuid.own_2 SELECT toDate('2024-01-01');
    SELECT count() FROM ${CLICKHOUSE_DATABASE}_nouuid.own_2;
    DROP DATABASE ${CLICKHOUSE_DATABASE}_nouuid SYNC;
"

#### 11 - Publishing the table to the database is the boundary for the rollback

${CLICKHOUSE_CLIENT} -q "
    SYSTEM ENABLE FAILPOINT database_on_disk_fail_before_commit_create_table;
    CREATE TABLE test_before_commit (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/before_commit', 'r1') ORDER BY date;
" 2>&1 | grep -cm1 "Fault injected (before"

# Nothing was published, so the registration is this statement's to remove and the retry succeeds
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='test_before_commit';
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/before_commit/replicas';
    CREATE TABLE test_before_commit (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/before_commit', 'r1') ORDER BY date;
    INSERT INTO test_before_commit SELECT toDate('2024-01-01');
    SELECT count() FROM test_before_commit;
    DROP TABLE test_before_commit SYNC;
    SYSTEM ENABLE FAILPOINT database_on_disk_fail_after_commit_create_table;
"

${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_after_commit (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/after_commit', 'r1') ORDER BY date" 2>&1 | grep -cm1 "Fault injected (after"

# The database accepted the table, so its data and registration must stay: it is reachable and
# recovers with DETACH/ATTACH, and DROP TABLE cleans up both.
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='test_after_commit';
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/after_commit/replicas';
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/after_commit' AND name='metadata';
    DETACH TABLE test_after_commit;
    ATTACH TABLE test_after_commit;
    INSERT INTO test_after_commit SELECT toDate('2024-01-01');
    SELECT count() FROM test_after_commit;
    DROP TABLE test_after_commit SYNC;
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/after_commit/replicas';
"

#### 12 - The rollback also removes the zero-copy lock root, which lives outside the table subtree

# The zero-copy root is created during the constructor, so a rollback that only removed the table
# subtree would orphan it under a path shared by all tables. Its own root keeps this arm independent
# of the default one, which is shared with every other table on the server.
ZC_ROOT="/clickhouse/zero_copy_$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX"

${CLICKHOUSE_CLIENT} -q "
    SYSTEM ENABLE FAILPOINT replicated_merge_tree_fail_after_creating_replica;
    CREATE TABLE test_zero_copy (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/zero_copy', 'r1') ORDER BY date
        SETTINGS storage_policy='local_cache', allow_remote_fs_zero_copy_replication=1,
                 remote_fs_zero_copy_zookeeper_path='$ZC_ROOT';
" 2>&1 | grep -cm1 "Fault injected (after creating replica)"

# The table subtree is gone, and so is the per-table lock node under the zero-copy root. The root
# itself is shared infrastructure that a plain DROP TABLE does not remove either. The retry then
# succeeds, and its own drop leaves the same state.
${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 -q "
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX' AND name='zero_copy';
    SELECT count() FROM system.zookeeper WHERE path='$ZC_ROOT/zero_copy_local_blob_storage';
    CREATE TABLE test_zero_copy (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/zero_copy', 'r1') ORDER BY date
        SETTINGS storage_policy='local_cache', allow_remote_fs_zero_copy_replication=1,
                 remote_fs_zero_copy_zookeeper_path='$ZC_ROOT';
    SELECT count() FROM system.zookeeper WHERE path='$ZC_ROOT/zero_copy_local_blob_storage';
    DROP TABLE test_zero_copy SYNC;
    SELECT count() FROM system.zookeeper WHERE path='$ZC_ROOT/zero_copy_local_blob_storage';
"

#### 13 - The rollback runs even when the zero-copy lock paths cannot be named

# An unresolvable macro in the zero-copy path fails the CREATE after the replica is registered, and
# naming those paths is part of the rollback itself. Naming them must not be what decides whether
# the registration is removed.
${CLICKHOUSE_CLIENT} \
    -q "CREATE TABLE test_zero_copy_unnamed (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/zero_copy_unnamed', 'r1') ORDER BY date
        SETTINGS storage_policy='local_cache', allow_remote_fs_zero_copy_replication=1,
                 remote_fs_zero_copy_zookeeper_path='$ZC_ROOT/{no_such_macro}'" 2>&1 | grep -cm1 "No macro 'no_such_macro' in config"

# The retry with a resolvable path then succeeds
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.zookeeper WHERE path='/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX' AND name='zero_copy_unnamed';
    CREATE TABLE test_zero_copy_unnamed (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/zero_copy_unnamed', 'r1') ORDER BY date
        SETTINGS storage_policy='local_cache', allow_remote_fs_zero_copy_replication=1,
                 remote_fs_zero_copy_zookeeper_path='$ZC_ROOT';
    INSERT INTO test_zero_copy_unnamed SELECT toDate('2024-01-01');
    SELECT count() FROM test_zero_copy_unnamed;
    DROP TABLE test_zero_copy_unnamed SYNC;
"

# No failpoint may leak into a later run of this test
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.fail_points WHERE enabled AND name IN ('replicated_merge_tree_fail_after_creating_replica', 'database_on_disk_fail_before_commit_create_table', 'database_on_disk_fail_after_commit_create_table')"
