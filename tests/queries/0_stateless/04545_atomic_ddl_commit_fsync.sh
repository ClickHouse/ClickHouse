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

# DirectorySync for a query, or -1 if the query_log row is not found yet.
directory_sync() {
    local query_id="$1"
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs query_log;
        select ProfileEvents['DirectorySync']
        from system.query_log
        where
            event_date >= yesterday() and event_time >= now() - 600 and
            current_database = currentDatabase() and
            query_id = {query_id:String} and
            type = 'QueryFinish'
        order by event_time_microseconds desc
        limit 1;
    "
}

check_ge() {
    local query_id="$1" expected="$2" what="$3"
    local got
    got=$(directory_sync "$query_id")
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
nofsync_id="nofsync_${tag}"

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists t_${tag} sync;
    drop table if exists t2_${tag} sync;
    drop table if exists a_${tag} sync;
    drop table if exists b_${tag} sync;
    drop table if exists c_${tag} sync;
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
$CLICKHOUSE_CLIENT --query_id "$drop_id" --fsync_metadata 1 -q \
    "drop table t2_${tag}"
# UNDROP: the inverse move (metadata_dropped -> database metadata), two directories.
$CLICKHOUSE_CLIENT --query_id "$undrop_id" --fsync_metadata 1 -q \
    "undrop table t2_${tag}"

check_ge "$create_id"   1 "CREATE"   || exit 2
check_ge "$alter_id"    1 "ALTER"    || exit 3
check_ge "$rename_id"   1 "RENAME"   || exit 4
check_ge "$exchange_id" 1 "EXCHANGE" || exit 5
check_ge "$drop_id"     3 "DROP"     || exit 6
check_ge "$undrop_id"   2 "UNDROP"   || exit 7

# The commit rename must NOT be forced to sync when fsync_metadata = 0.
$CLICKHOUSE_CLIENT -q "create table c_${tag} (id UInt64) engine=MergeTree order by id"
$CLICKHOUSE_CLIENT --query_id "$nofsync_id" --fsync_metadata 0 -q "drop table c_${tag}"
nofsync_dir_sync=$(directory_sync "$nofsync_id")
if [[ "$nofsync_dir_sync" != "0" ]]; then
    echo "fsync_metadata=0 not honored on DROP: DirectorySync=$nofsync_dir_sync" >&2
    exit 8
fi

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists t2_${tag} sync;
    drop table if exists a_${tag} sync;
    drop table if exists b_${tag} sync;
"

echo "OK"
