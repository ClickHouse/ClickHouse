#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-fasttest: an encrypted disk requires USE_SSL
# no-object-storage: the default disk must not contribute DirectorySync events
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique per run: a custom disk is cached under a name hashed from its definition, and nothing
# evicts it, so a rerun against the same server with a fixed database would reuse the disk, create
# no root and measure zero. A fixed query_id would likewise match the older run's query_log row,
# and the multi-line result then breaks the numeric comparisons below.
RUN_ID="$(random_str 10)"
BASE="${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${RUN_ID}"
ENC_BASE="disk(type=local, path='${BASE}/enc_base/')"

directory_syncs()
{
    $CLICKHOUSE_CLIENT -m --param_query_id "$1" -q "
        system flush logs query_log;
        select ProfileEvents['DirectorySync'] from system.query_log
        where query_id = {query_id:String} and current_database = currentDatabase() and type = 'QueryFinish'
        order by event_time_microseconds desc limit 1;
    "
}

# Exact, not a lower bound: the only producer of DirectorySync is the LocalDirectorySyncGuard
# destructor, and every other consumer a CREATE TABLE could reach is behind `fsync_part_directory`,
# which defaults to false and is not randomized. So dropping one required fsync fails here.
# An empty result must never satisfy a comparison: `[[ "" -eq 0 ]]` is true.
synced_exactly()
{
    local actual="$1" expected="$2"
    [[ -n "$actual" && "$actual" -eq "$expected" ]]
}

# A newly created local disk root: every created level's parent must be synced. `${BASE}` does not
# exist either, so three levels are created (`${BASE}`, `a`, `b`) and three parents are synced.
qid="local-$CLICKHOUSE_DATABASE-$RUN_ID"
$CLICKHOUSE_CLIENT --query_id "$qid" -q "
    create table t_local (a Int32) engine = MergeTree order by tuple()
    settings disk = disk(type=local, path='${BASE}/a/b/');
"
synced_exactly "$(directory_syncs "$qid")" 3 && echo 'local root synced' || echo 'local root NOT synced'

# Create the disk wrapped by the encrypted disks below, so the counts that follow
# only contain the syncs done for the encrypted prefix itself.
$CLICKHOUSE_CLIENT -q "
    create table t_enc_base (a Int32) engine = MergeTree order by tuple()
    settings disk = ${ENC_BASE};
"

# A one-level encrypted prefix: the directory that holds its entry is the wrapped disk's root.
qid="enc-one-level-$CLICKHOUSE_DATABASE-$RUN_ID"
$CLICKHOUSE_CLIENT --query_id "$qid" -q "
    create table t_enc_one (a Int32) engine = MergeTree order by tuple()
    settings disk = disk(type=encrypted, key='1234567812345678', disk=${ENC_BASE}, path='enc1/');
"
synced_exactly "$(directory_syncs "$qid")" 1 && echo 'encrypted root synced' || echo 'encrypted root NOT synced'

# A nested encrypted prefix: both the prefix and the wrapped disk's root hold a new entry.
qid="enc-nested-$CLICKHOUSE_DATABASE-$RUN_ID"
$CLICKHOUSE_CLIENT --query_id "$qid" -q "
    create table t_enc_nested (a Int32) engine = MergeTree order by tuple()
    settings disk = disk(type=encrypted, key='1234567812345678', disk=${ENC_BASE}, path='enc2/sub/');
"
synced_exactly "$(directory_syncs "$qid")" 2 && echo 'encrypted nested synced' || echo 'encrypted nested NOT synced'

# A remote wrapped disk cannot synchronize a directory, so nothing is attempted for it.
qid="enc-remote-$CLICKHOUSE_DATABASE-$RUN_ID"
$CLICKHOUSE_CLIENT --query_id "$qid" -q "
    create table t_enc_remote (a Int32) engine = MergeTree order by tuple()
    settings disk = disk(type=encrypted, key='1234567812345678',
        disk=disk(type=object_storage, object_storage_type=local_blob_storage, path='${BASE}/obj/'),
        path='enc/sub/');
"
remote_syncs="$(directory_syncs "$qid")"
[[ -n "$remote_syncs" && "$remote_syncs" -eq 0 ]] && echo 'remote delegate not synced' || echo 'remote delegate synced'

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
