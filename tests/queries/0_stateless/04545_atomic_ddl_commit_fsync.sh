#!/usr/bin/env bash
# Tags: atomic-database, no-object-storage, no-shared-catalog
# Tag atomic-database: the fix and these DirectorySync counts are specific to the Atomic
#   database engine (Ordinary/Replicated go through other metadata-commit paths).
# Tag no-object-storage: object storage disks do not fsync directories.
# Tag no-shared-catalog: metadata lives in Keeper, not in local .sql files.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111348
# The on-disk commit of DROP / RENAME / EXCHANGE / UNDROP TABLE on an Atomic database is a
# rename of the table's .sql metadata file. That rename used to be issued without fsyncing
# the parent directory, so a power loss inside the filesystem journal-commit window could
# silently revert an acknowledged DDL. The fix syncs the directory (honoring fsync_metadata);
# assert the DirectorySync ProfileEvent is emitted for each committed rename.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# DirectorySync per query_id; an unset entry means no query_log row was found.
declare -A directory_sync

# `system flush logs` is server-wide and serializes against every concurrently running test,
# so a whole batch of query_ids is read through a single flush.
collect_directory_sync() {
    local ids="['$1'"; shift
    for id in "$@"; do ids+=",'$id'"; done
    ids+="]"

    local query_id got
    while IFS=$'\t' read -r query_id got; do
        [[ -n "$query_id" ]] && directory_sync["$query_id"]="$got"
    done < <($CLICKHOUSE_CLIENT -m --param_ids "$ids" -q "
        system flush logs query_log;
        select query_id, argMax(ProfileEvents['DirectorySync'], event_time_microseconds)
        from system.query_log
        where
            event_date >= yesterday() and event_time >= now() - 600 and
            current_database = currentDatabase() and
            has({ids:Array(String)}, query_id) and
            type = 'QueryFinish'
        group by query_id
        -- Aggregation must stay local: a randomized parallel-replicas setting would make this
        -- read require a configured cluster.
        settings enable_parallel_replicas = 0
        format TSV;
    ")
}

check_ge() {
    local query_id="$1" expected="$2" what="$3"
    local got="${directory_sync[$query_id]-}"
    if [[ "${got:--1}" -lt "$expected" ]]; then
        echo "$what: DirectorySync=$got, expected >= $expected" >&2
        return 1
    fi
    return 0
}

# currentDatabase() is an Atomic database whose metadata is stored on the local disk.
# Use a token unique to this invocation so re-runs (--test-runs) never reuse a query_id
# or a table name in the (reused) test database.
tag="${CLICKHOUSE_DATABASE}_$$"

create_id="create_${tag}"
alter_id="alter_${tag}"
rename_id="rename_${tag}"
exchange_id="exchange_${tag}"
drop_id="drop_${tag}"
undrop_id="undrop_${tag}"
# Cross-database RENAME syncs two distinct directories (source db + target db).
rename_xdb_id="rename_xdb_${tag}"
# Database-level metadata commits are the same durability class (#111348).
create_db_id="create_db_${tag}"
rename_db_id="rename_db_${tag}"
altercomment_db_id="altercomment_db_${tag}"
drop_db_id="drop_db_${tag}"
# Negative path: every commit rename/unlink is gated on fsync_metadata; with it off none may sync.
# Cover one query from each INDEPENDENT gate so making any single gate unconditional fails here.
nofsync_drop_id="nofsync_drop_${tag}"
nofsync_create_id="nofsync_create_${tag}"
nofsync_rename_id="nofsync_rename_${tag}"
nofsync_alter_id="nofsync_alter_${tag}"
nofsync_undrop_id="nofsync_undrop_${tag}"
nofsync_createdb_id="nofsync_createdb_${tag}"
nofsync_altercommentdb_id="nofsync_altercommentdb_${tag}"
nofsync_renamedb_id="nofsync_renamedb_${tag}"
nofsync_dropdb_id="nofsync_dropdb_${tag}"

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists t_${tag} sync;
    drop table if exists t2_${tag} sync;
    drop table if exists a_${tag} sync;
    drop table if exists b_${tag} sync;
    drop table if exists c_${tag} sync;
    drop table if exists c2_${tag} sync;
    drop database if exists db_${tag} sync;
    drop database if exists db2_${tag} sync;
    drop database if exists db3_${tag} sync;
    drop database if exists db4_${tag} sync;
    drop database if exists dbsrc_${tag} sync;
    drop table if exists x_${tag} sync;
"

# CREATE: the .sql content is already fsynced by the writer; the tmp -> .sql commit
# rename must sync its (single) directory.
$CLICKHOUSE_CLIENT --query_id "$create_id" --fsync_metadata 1 -q \
    "create table t_${tag} (id UInt64) engine=MergeTree order by id"
# ALTER: also commits via a tmp -> .sql rename in the same directory.
$CLICKHOUSE_CLIENT --query_id "$alter_id" --fsync_metadata 1 -q \
    "alter table t_${tag} add column s String"
# RENAME within the same database: source and target share one directory.
$CLICKHOUSE_CLIENT --query_id "$rename_id" --fsync_metadata 1 -q \
    "rename table t_${tag} to t2_${tag}"
# EXCHANGE within the same database: one directory.
$CLICKHOUSE_CLIENT -m -q "
    create table a_${tag} (id UInt64) engine=MergeTree order by id;
    create table b_${tag} (id UInt64) engine=MergeTree order by id;
"
$CLICKHOUSE_CLIENT --query_id "$exchange_id" --fsync_metadata 1 -q \
    "exchange tables a_${tag} and b_${tag}"
# DROP: the .sql moves from the database metadata directory into the shared metadata_dropped
# directory. Three distinct directories are synced: the source, metadata_dropped, and
# metadata_dropped's own parent (the disk root) -- the last one guards the custom-metadata-disk
# case where metadata_dropped may have been created without its own entry fsync'd. Require >= 3
# so dropping that third guard (the exact regression being fixed) fails the test.
# Pin an async drop so the table lingers in the dropped queue for the UNDROP below (the CI
# default profile sets this to 1, which finalizes the drop before UNDROP can run). The three
# directory syncs are gated on fsync_metadata, not on this flag, so the count is unchanged.
$CLICKHOUSE_CLIENT --query_id "$drop_id" --fsync_metadata 1 \
    --database_atomic_wait_for_drop_and_detach_synchronously 0 -q \
    "drop table t2_${tag}"
# UNDROP: the inverse move (metadata_dropped -> database metadata), two directories.
$CLICKHOUSE_CLIENT --query_id "$undrop_id" --fsync_metadata 1 -q \
    "undrop table t2_${tag}"

# Database-level metadata commits are renames of the `<db>.sql` file, the same durability class.
# CREATE DATABASE: the tmp -> `<db>.sql` commit rename must sync its (single) directory.
$CLICKHOUSE_CLIENT --query_id "$create_db_id" --fsync_metadata 1 -q \
    "create database db_${tag} engine=Atomic"
# ALTER DATABASE MODIFY COMMENT: commits a metadata update via a tmp -> `<db>.sql` replace.
$CLICKHOUSE_CLIENT --query_id "$altercomment_db_id" --fsync_metadata 1 -q \
    "alter database db_${tag} modify comment 'c'"
# RENAME DATABASE: `<db>.sql` moves within the metadata directory (single directory).
$CLICKHOUSE_CLIENT --query_id "$rename_db_id" --fsync_metadata 1 -q \
    "rename database db_${tag} to db2_${tag}"
# DROP DATABASE: the `<db>.sql` unlink is the on-disk commit; its directory must be synced.
$CLICKHOUSE_CLIENT --query_id "$drop_db_id" --fsync_metadata 1 -q \
    "drop database db2_${tag} sync"
# Cross-database RENAME: source and target live in different database metadata directories, so
# both must be synced (>= 2). Guards against a regression that syncs only the source directory.
$CLICKHOUSE_CLIENT -m -q "
    create database dbsrc_${tag} engine=Atomic;
    create table dbsrc_${tag}.x (id UInt64) engine=MergeTree order by id;
"
$CLICKHOUSE_CLIENT --query_id "$rename_xdb_id" --fsync_metadata 1 -q \
    "rename table dbsrc_${tag}.x to ${CLICKHOUSE_DATABASE}.x_${tag}"

collect_directory_sync "$create_id" "$alter_id" "$rename_id" "$exchange_id" "$drop_id" \
    "$undrop_id" "$create_db_id" "$altercomment_db_id" "$rename_db_id" "$drop_db_id" \
    "$rename_xdb_id"

check_ge "$create_id"          1 "CREATE"            || exit 2
check_ge "$alter_id"           1 "ALTER"             || exit 3
check_ge "$rename_id"          1 "RENAME"            || exit 4
check_ge "$exchange_id"        1 "EXCHANGE"          || exit 5
check_ge "$drop_id"            3 "DROP"              || exit 6
check_ge "$undrop_id"          2 "UNDROP"            || exit 7
check_ge "$create_db_id"       1 "CREATE DATABASE"   || exit 9
check_ge "$altercomment_db_id" 1 "ALTER DATABASE"    || exit 10
check_ge "$rename_db_id"       1 "RENAME DATABASE"   || exit 11
check_ge "$drop_db_id"         1 "DROP DATABASE"     || exit 16
check_ge "$rename_xdb_id"      2 "RENAME CROSS-DB"   || exit 21

# Every commit rename above is gated on fsync_metadata; with it off none of them must force a
# directory sync. Cover a representative rename from each guarded family (not just DROP) so that
# silently dropping any one of the `if (fsync_metadata)` gates later fails this test.
check_nofsync() {
    local query_id="$1" what="$2"
    local got="${directory_sync[$query_id]-}"
    if [[ "$got" != "0" ]]; then
        echo "fsync_metadata=0 not honored on $what: DirectorySync=$got" >&2
        return 1
    fi
    return 0
}

# Table-metadata gates: CREATE, ALTER, RENAME, DROP, UNDROP.
$CLICKHOUSE_CLIENT --query_id "$nofsync_create_id" --fsync_metadata 0 -q \
    "create table c_${tag} (id UInt64) engine=MergeTree order by id"
$CLICKHOUSE_CLIENT --query_id "$nofsync_alter_id" --fsync_metadata 0 -q \
    "alter table c_${tag} add column s String"
$CLICKHOUSE_CLIENT --query_id "$nofsync_rename_id" --fsync_metadata 0 -q \
    "rename table c_${tag} to c2_${tag}"
$CLICKHOUSE_CLIENT --query_id "$nofsync_drop_id" --fsync_metadata 0 \
    --database_atomic_wait_for_drop_and_detach_synchronously 0 -q \
    "drop table c2_${tag}"
$CLICKHOUSE_CLIENT --query_id "$nofsync_undrop_id" --fsync_metadata 0 -q \
    "undrop table c2_${tag}"
# Database-metadata gates: CREATE, ALTER COMMENT, RENAME, DROP DATABASE. These commit via the
# database-catalog metadata-update / rename / unlink paths, which historically read the global
# setting -- assert each honors the query-level fsync_metadata.
$CLICKHOUSE_CLIENT --query_id "$nofsync_createdb_id" --fsync_metadata 0 -q \
    "create database db3_${tag} engine=Atomic"
$CLICKHOUSE_CLIENT --query_id "$nofsync_altercommentdb_id" --fsync_metadata 0 -q \
    "alter database db3_${tag} modify comment 'c'"
$CLICKHOUSE_CLIENT --query_id "$nofsync_renamedb_id" --fsync_metadata 0 -q \
    "rename database db3_${tag} to db4_${tag}"
$CLICKHOUSE_CLIENT --query_id "$nofsync_dropdb_id" --fsync_metadata 0 -q \
    "drop database db4_${tag} sync"

collect_directory_sync "$nofsync_create_id" "$nofsync_alter_id" "$nofsync_rename_id" \
    "$nofsync_drop_id" "$nofsync_undrop_id" "$nofsync_createdb_id" \
    "$nofsync_altercommentdb_id" "$nofsync_renamedb_id" "$nofsync_dropdb_id"

check_nofsync "$nofsync_create_id"         "CREATE"                 || exit 12
check_nofsync "$nofsync_alter_id"          "ALTER"                  || exit 17
check_nofsync "$nofsync_rename_id"         "RENAME"                 || exit 13
check_nofsync "$nofsync_drop_id"           "DROP"                   || exit 8
check_nofsync "$nofsync_undrop_id"         "UNDROP"                 || exit 18
check_nofsync "$nofsync_createdb_id"       "CREATE DATABASE"        || exit 14
check_nofsync "$nofsync_altercommentdb_id" "ALTER DATABASE COMMENT" || exit 15
check_nofsync "$nofsync_renamedb_id"       "RENAME DATABASE"        || exit 19
check_nofsync "$nofsync_dropdb_id"         "DROP DATABASE"          || exit 20

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists t2_${tag} sync;
    drop table if exists a_${tag} sync;
    drop table if exists b_${tag} sync;
    drop table if exists c2_${tag} sync;
    drop database if exists db2_${tag} sync;
    drop database if exists db3_${tag} sync;
    drop database if exists db4_${tag} sync;
    drop database if exists dbsrc_${tag} sync;
    drop table if exists x_${tag} sync;
"

echo "OK"
