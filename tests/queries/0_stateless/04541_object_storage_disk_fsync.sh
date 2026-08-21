#!/usr/bin/env bash
# Tags: no-fasttest, no-random-merge-tree-settings
# no-fasttest: builds a custom object-storage disk
# no-random-merge-tree-settings: the test pins the fsync and part-format settings it compares

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An object-storage disk keeps a local metadata file per logical file of a part, on top of the blob
# holding the data, so for the same table and the same settings it has strictly more files to sync
# than a local disk, where the data file is the only file. Comparing the two disks keeps the
# assertion independent of how many files a part happens to have.
fsync_events() {
    local query_id=$1
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs query_log;
        select ProfileEvents['FileSync'], ProfileEvents['DirectorySync']
        from system.query_log
        where current_database = currentDatabase() and query_id = {query_id:String} and type = 'QueryFinish'
        order by event_time_microseconds desc limit 1;
    "
}

# args: part_kind (wide|compact), fsync (1|0), label
run_case() {
    local part_kind=$1 fsync=$2 label=$3
    local suffix="${part_kind}_${fsync}"
    local part_setting="min_bytes_for_wide_part = 0"
    [[ "$part_kind" == "compact" ]] && part_setting="min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 100000000"
    local fsync_setting=""
    [[ "$fsync" == "1" ]] && fsync_setting=", fsync_after_insert = 1"

    local disk_name="objd_${suffix}_${CLICKHOUSE_DATABASE}"

    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists local_${suffix};
        drop table if exists objstore_${suffix};
        create table local_${suffix} (id UInt64, v String) engine = MergeTree order by id
        settings ${part_setting}${fsync_setting};
        create table objstore_${suffix} (id UInt64, v String) engine = MergeTree order by id
        settings disk = disk(name = '${disk_name}', type = object_storage,
                             object_storage_type = local_blob_storage, path = '${disk_name}/'),
                 ${part_setting}${fsync_setting};
    "

    local local_id="local-${suffix}-$CLICKHOUSE_DATABASE"
    local objstore_id="objstore-${suffix}-$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT --query_id "$local_id" -q "insert into local_${suffix} select number, toString(number) from numbers(20000)"
    $CLICKHOUSE_CLIENT --query_id "$objstore_id" -q "insert into objstore_${suffix} select number, toString(number) from numbers(20000)"

    local local_files local_dirs objstore_files objstore_dirs
    read -r local_files local_dirs <<<"$(fsync_events "$local_id")"
    read -r objstore_files objstore_dirs <<<"$(fsync_events "$objstore_id")"

    if [[ "$fsync" == "1" ]]; then
        echo "${label}: local synced=$((local_files > 0)) objstore syncs more=$((objstore_files > local_files)) DirectorySync=$((objstore_dirs))"
    else
        echo "${label}: local FileSync=$((local_files)) objstore FileSync=$((objstore_files)) DirectorySync=$((objstore_dirs))"
    fi

    $CLICKHOUSE_CLIENT -m -q "
        select count(), sum(id) from objstore_${suffix};
        drop table local_${suffix};
        drop table objstore_${suffix};
    "
}

# With fsync_after_insert an object-storage disk must sync the local metadata files that commit the
# part on top of its blobs, so it issues strictly more syncs than a local disk holding the same table.
run_case wide 1 "wide, fsync on"
run_case compact 1 "compact, fsync on"

# Without the setting neither disk forces any sync.
run_case wide 0 "wide, fsync off"
run_case compact 0 "compact, fsync off"

# The merge path derives its own sync request from min_rows_to_fsync_after_merge /
# min_compressed_bytes_to_fsync_after_merge, so it needs its own case. args: merge_fsync (1|0), label
run_merge_case() {
    local merge_fsync=$1 label=$2
    local suffix="merge_${merge_fsync}"
    local merge_setting="min_rows_to_fsync_after_merge = 0, min_compressed_bytes_to_fsync_after_merge = 0"
    [[ "$merge_fsync" == "1" ]] && merge_setting="min_rows_to_fsync_after_merge = 1"

    # fsync_after_insert = 0 keeps the inserts out of the measurement so only the merge syncs.
    # max_bytes_to_merge_at_max_space_in_pool = 1 disables background merges, so OPTIMIZE FINAL
    # (which ignores that limit) is the only merger and its syncs are attributable to its query id.
    # min_bytes_for_full_part_storage = 0 is randomized in CI; a Packed part is a single blob and
    # would have no per-stream files to sync.
    local common="min_bytes_for_wide_part = 0, fsync_after_insert = 0,
                  max_bytes_to_merge_at_max_space_in_pool = 1, min_bytes_for_full_part_storage = 0,
                  ${merge_setting}"
    local disk_name="objd_${suffix}_${CLICKHOUSE_DATABASE}"

    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists local_${suffix};
        drop table if exists objstore_${suffix};
        create table local_${suffix} (id UInt64, v String) engine = MergeTree order by id
        settings ${common};
        create table objstore_${suffix} (id UInt64, v String) engine = MergeTree order by id
        settings disk = disk(name = '${disk_name}', type = object_storage,
                             object_storage_type = local_blob_storage, path = '${disk_name}/'),
                 ${common};
        insert into local_${suffix} select number, toString(number) from numbers(10000);
        insert into local_${suffix} select number + 10000, toString(number) from numbers(10000);
        insert into objstore_${suffix} select number, toString(number) from numbers(10000);
        insert into objstore_${suffix} select number + 10000, toString(number) from numbers(10000);
    "

    local local_id="local-${suffix}-$CLICKHOUSE_DATABASE"
    local objstore_id="objstore-${suffix}-$CLICKHOUSE_DATABASE"
    # optimize_throw_if_noop makes a merge that did not happen a loud failure instead of a 0 reading.
    $CLICKHOUSE_CLIENT --query_id "$local_id" -q "optimize table local_${suffix} final settings optimize_throw_if_noop = 1, alter_sync = 2"
    $CLICKHOUSE_CLIENT --query_id "$objstore_id" -q "optimize table objstore_${suffix} final settings optimize_throw_if_noop = 1, alter_sync = 2"

    local local_files local_dirs objstore_files objstore_dirs
    read -r local_files local_dirs <<<"$(fsync_events "$local_id")"
    read -r objstore_files objstore_dirs <<<"$(fsync_events "$objstore_id")"

    if [[ "$merge_fsync" == "1" ]]; then
        echo "${label}: local synced=$((local_files > 0)) objstore syncs more=$((objstore_files > local_files))"
    else
        echo "${label}: local FileSync=$((local_files)) objstore FileSync=$((objstore_files))"
    fi

    $CLICKHOUSE_CLIENT -m -q "
        select count(), sum(id) from objstore_${suffix};
        drop table local_${suffix};
        drop table objstore_${suffix};
    "
}

# With min_rows_to_fsync_after_merge the merged part's local metadata must be synced too, so the
# object-storage disk again issues strictly more syncs than the local disk for the same merge.
run_merge_case 1 "merge, fsync on"
# With both merge thresholds at 0 neither disk syncs anything during the merge.
run_merge_case 0 "merge, fsync off"

# fsync_part_directory alone syncs nothing on these disks, which is what a local disk does for
# fsync_after_insert = 0: the directory sync is a separate setting from the file contents sync.
$CLICKHOUSE_CLIENT -m -q "
    drop table if exists dir_only;
    create table dir_only (id UInt64, v String) engine = MergeTree order by id
    settings disk = disk(name = 'objd_dir_${CLICKHOUSE_DATABASE}', type = object_storage,
                         object_storage_type = local_blob_storage, path = 'objd_dir_${CLICKHOUSE_DATABASE}/'),
             min_bytes_for_wide_part = 0, fsync_after_insert = 0, fsync_part_directory = 1;
"
$CLICKHOUSE_CLIENT --query_id "dironly-$CLICKHOUSE_DATABASE" -q "insert into dir_only select number, toString(number) from numbers(20000)"
read -r dir_only_files dir_only_dirs <<<"$(fsync_events "dironly-$CLICKHOUSE_DATABASE")"
echo "wide, dir only: FileSync=$((dir_only_files)) DirectorySync=$((dir_only_dirs))"
$CLICKHOUSE_CLIENT -q "drop table dir_only"
