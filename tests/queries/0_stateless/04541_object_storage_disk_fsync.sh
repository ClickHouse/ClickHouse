#!/usr/bin/env bash
# Tags: no-fasttest, no-random-merge-tree-settings
# no-fasttest: builds a custom object-storage disk
# no-random-merge-tree-settings: the test pins the fsync settings it verifies

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111330
# Object-storage disks used to silently ignore the MergeTree fsync setting family: an INSERT with
# fsync_after_insert=1 / fsync_part_directory=1 issued zero fsync, so an acknowledged part (and the
# local metadata files that commit it) was lost on power loss even when the blobs were durable.
#
# We assert the fsync path is taken (rather than simulating power loss) via the FileSync /
# DirectorySync ProfileEvents of the INSERT: the part write, its local metadata commit and the
# part-directory sync run synchronously on the query thread. Both wide and compact parts are covered
# because a compact part syncs its stream before finalizing (the metadata file does not exist yet)
# while a wide part syncs after - the two exercise different branches of the fix.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

fsync_events() {
    local query_id=$1
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs query_log;
        select ProfileEvents['FileSync'] > 0, ProfileEvents['DirectorySync'] > 0
        from system.query_log
        where current_database = currentDatabase() and query_id = {query_id:String} and type = 'QueryFinish'
        order by event_time_microseconds desc limit 1;
    "
}

# args: part_kind (wide|compact), fsync (1|0), label
run_case() {
    local part_kind=$1 fsync=$2 label=$3
    local disk_name="objd_${part_kind}_${CLICKHOUSE_DATABASE}"
    local disk_def="disk = disk(name = '${disk_name}', type = object_storage, object_storage_type = local_blob_storage, path = 'objd_${part_kind}_${CLICKHOUSE_DATABASE}/')"
    local part_setting="min_bytes_for_wide_part = 0"
    [[ "$part_kind" == "compact" ]] && part_setting="min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 100000000"
    local fsync_settings=""
    [[ "$fsync" == "1" ]] && fsync_settings=", fsync_after_insert = 1, fsync_part_directory = 1"

    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists t_${part_kind};
        create table t_${part_kind} (id UInt64, v UInt64) engine = MergeTree order by id
        settings ${disk_def}, ${part_setting}${fsync_settings};
    "

    # A too-fast fdatasync leaves *ElapsedMicroseconds at 0, but FileSync / DirectorySync (the call
    # counters) are deterministic once the path is taken, so retry only the fsync=1 case.
    local attempts=1
    [[ "$fsync" == "1" ]] && attempts=30
    local file_sync=0 dir_sync=0
    for i in $(seq 1 $attempts); do
        local query_id="ins-${part_kind}-${fsync}-$i-$CLICKHOUSE_DATABASE"
        $CLICKHOUSE_CLIENT --query_id "$query_id" -q "insert into t_${part_kind} select number, number from numbers(1000)"
        read -r file_sync dir_sync <<<"$(fsync_events "$query_id")"
        { [[ "$fsync" == "1" && "$file_sync" == "1" && "$dir_sync" == "1" ]] || [[ "$fsync" == "0" ]]; } && break
    done
    echo "${label}: FileSync>0=${file_sync} DirectorySync>0=${dir_sync}"
    $CLICKHOUSE_CLIENT -q "drop table t_${part_kind}"
}

# A queued (non-fake) transaction, as used by Keeper metadata, must still accept an INSERT with the
# fsync settings (the directory/metadata sync must not try to open not-yet-created paths). Local
# fsync is not expected there - Keeper owns durability - so we only assert the insert succeeds.
run_nonfake() {
    local disk_def="disk = disk(name = 'objd_nf_${CLICKHOUSE_DATABASE}', type = object_storage, object_storage_type = local_blob_storage, path = 'objd_nf_${CLICKHOUSE_DATABASE}/', use_fake_transaction = false)"
    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists t_nonfake;
        create table t_nonfake (id UInt64, v UInt64) engine = MergeTree order by id
        settings ${disk_def}, min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_part_directory = 1;
        insert into t_nonfake select number, number from numbers(1000);
    "
    echo "non-fake txn, fsync on: $($CLICKHOUSE_CLIENT -q "select count(), sum(v) from t_nonfake")"
    $CLICKHOUSE_CLIENT -q "drop table t_nonfake"
}

# With the fsync settings the local metadata files and the part directory must be synced, for wide
# and compact parts alike.
run_case wide 1 "wide, fsync on"
run_case compact 1 "compact, fsync on"

# Without the fsync settings (defaults) there must be no forced fsync: the fix is strictly opt-in.
run_case wide 0 "wide, fsync off"

run_nonfake
