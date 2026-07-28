#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-fasttest: an encrypted disk requires USE_SSL
# no-object-storage: the default disk must not contribute DirectorySync events
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
ENC_BASE="disk(type=local, path='${BASE}/enc_base/')"

directory_syncs()
{
    $CLICKHOUSE_CLIENT -m --param_query_id "$1" -q "
        system flush logs query_log;
        select ProfileEvents['DirectorySync'] from system.query_log
        where query_id = {query_id:String} and current_database = currentDatabase() and type = 'QueryFinish';
    "
}

# A newly created local disk root: every created level's parent must be synced.
qid="local-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id "$qid" -q "
    create table t_local (a Int32) engine = MergeTree order by tuple()
    settings disk = disk(type=local, path='${BASE}/a/b/');
"
[[ "$(directory_syncs "$qid")" -ge 2 ]] && echo 'local root synced' || echo 'local root NOT synced'

# Create the disk wrapped by the encrypted disks below, so the counts that follow
# only contain the syncs done for the encrypted prefix itself.
$CLICKHOUSE_CLIENT -q "
    create table t_enc_base (a Int32) engine = MergeTree order by tuple()
    settings disk = ${ENC_BASE};
"

# A one-level encrypted prefix: the directory that holds its entry is the wrapped disk's root.
qid="enc-one-level-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id "$qid" -q "
    create table t_enc_one (a Int32) engine = MergeTree order by tuple()
    settings disk = disk(type=encrypted, key='1234567812345678', disk=${ENC_BASE}, path='enc1/');
"
[[ "$(directory_syncs "$qid")" -ge 1 ]] && echo 'encrypted root synced' || echo 'encrypted root NOT synced'

# A nested encrypted prefix: both the prefix and the wrapped disk's root hold a new entry.
qid="enc-nested-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id "$qid" -q "
    create table t_enc_nested (a Int32) engine = MergeTree order by tuple()
    settings disk = disk(type=encrypted, key='1234567812345678', disk=${ENC_BASE}, path='enc2/sub/');
"
[[ "$(directory_syncs "$qid")" -ge 2 ]] && echo 'encrypted nested synced' || echo 'encrypted nested NOT synced'

# A remote wrapped disk cannot synchronize a directory, so nothing is attempted for it.
qid="enc-remote-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id "$qid" -q "
    create table t_enc_remote (a Int32) engine = MergeTree order by tuple()
    settings disk = disk(type=encrypted, key='1234567812345678',
        disk=disk(type=object_storage, object_storage_type=local_blob_storage, path='${BASE}/obj/'),
        path='enc/sub/');
"
[[ "$(directory_syncs "$qid")" -eq 0 ]] && echo 'remote delegate not synced' || echo 'remote delegate synced'

# Every disk is still usable.
$CLICKHOUSE_CLIENT -m -q "
    insert into t_local values (1);
    insert into t_enc_one values (2);
    insert into t_enc_nested values (3);
    insert into t_enc_remote values (4);
    select count() from t_local;
    select count() from t_enc_one;
    select count() from t_enc_nested;
    select count() from t_enc_remote;

    drop table t_local;
    drop table t_enc_base;
    drop table t_enc_one;
    drop table t_enc_nested;
    drop table t_enc_remote;
"
