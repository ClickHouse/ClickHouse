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
#
# Each measured statement carries a log_comment naming its case, and every case is read back in one
# pass at the end, so a case must not assert anything itself.

# args: part_kind (wide|compact), fsync (1|0)
run_case() {
    local part_kind=$1 fsync=$2
    local suffix="${part_kind}_${fsync}"
    local part_setting="min_bytes_for_wide_part = 0"
    [[ "$part_kind" == "compact" ]] && part_setting="min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 100000000"
    local fsync_setting=""
    [[ "$fsync" == "1" ]] && fsync_setting=", fsync_after_insert = 1"

    local disk_name="objd_${suffix}_${CLICKHOUSE_DATABASE}"

    $CLICKHOUSE_CLIENT -m -q "
        create table local_${suffix} (id UInt64, v String) engine = MergeTree order by id
        settings ${part_setting}${fsync_setting};
        create table objstore_${suffix} (id UInt64, v String) engine = MergeTree order by id
        settings disk = disk(name = '${disk_name}', type = object_storage,
                             object_storage_type = local_blob_storage, path = '${disk_name}/'),
                 ${part_setting}${fsync_setting};
        insert into local_${suffix} select number, toString(number) from numbers(2000)
        settings log_comment = 'c-local-${suffix}';
        insert into objstore_${suffix} select number, toString(number) from numbers(2000)
        settings log_comment = 'c-objstore-${suffix}';
    "
}

# The merge path derives its own sync request from min_rows_to_fsync_after_merge /
# min_compressed_bytes_to_fsync_after_merge, so it needs its own case. args: merge_fsync (1|0)
run_merge_case() {
    local merge_fsync=$1
    local suffix="merge_${merge_fsync}"
    local merge_setting="min_rows_to_fsync_after_merge = 0, min_compressed_bytes_to_fsync_after_merge = 0"
    [[ "$merge_fsync" == "1" ]] && merge_setting="min_rows_to_fsync_after_merge = 1"

    # fsync_after_insert = 0 keeps the inserts out of the measurement so only the merge syncs.
    # max_bytes_to_merge_at_max_space_in_pool = 1 disables background merges, so OPTIMIZE FINAL
    # (which ignores that limit) is the only merger and its syncs are attributable to its log comment.
    # min_bytes_for_full_part_storage = 0 is randomized in CI; a Packed part is a single blob and
    # would have no per-stream files to sync.
    local common="min_bytes_for_wide_part = 0, fsync_after_insert = 0,
                  max_bytes_to_merge_at_max_space_in_pool = 1, min_bytes_for_full_part_storage = 0,
                  ${merge_setting}"
    local disk_name="objd_${suffix}_${CLICKHOUSE_DATABASE}"

    # optimize_throw_if_noop makes a merge that did not happen a loud failure instead of a 0 reading.
    $CLICKHOUSE_CLIENT -m -q "
        create table local_${suffix} (id UInt64, v String) engine = MergeTree order by id
        settings ${common};
        create table objstore_${suffix} (id UInt64, v String) engine = MergeTree order by id
        settings disk = disk(name = '${disk_name}', type = object_storage,
                             object_storage_type = local_blob_storage, path = '${disk_name}/'),
                 ${common};
        insert into local_${suffix} select number, toString(number) from numbers(1000);
        insert into local_${suffix} select number + 1000, toString(number) from numbers(1000);
        insert into objstore_${suffix} select number, toString(number) from numbers(1000);
        insert into objstore_${suffix} select number + 1000, toString(number) from numbers(1000);
        optimize table local_${suffix} final
        settings optimize_throw_if_noop = 1, alter_sync = 2, log_comment = 'c-local-${suffix}';
        optimize table objstore_${suffix} final
        settings optimize_throw_if_noop = 1, alter_sync = 2, log_comment = 'c-objstore-${suffix}';
    "
}

# A mutation entry is written straight through the disk rather than through a part transaction, so it
# commits its own metadata file instead of having commit do it, and needs its own case. The entry
# is always synced, so there is no setting that turns this off and no negative arm to pair with it.
run_alter_case() {
    local suffix="alter"
    # fsync_after_insert = 0 and the merge thresholds at their non-triggering values keep the inserts
    # and any merge out of the measurement, so the only sync attributable to the ALTER is the
    # mutation entry's. min_bytes_for_wide_part / min_bytes_for_full_part_storage are pinned for
    # the same reason run_merge_case pins them: CI randomization would change the file count.
    local common="min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
                  fsync_after_insert = 0, min_rows_to_fsync_after_merge = 0,
                  min_compressed_bytes_to_fsync_after_merge = 0"
    local disk_name="objd_${suffix}_${CLICKHOUSE_DATABASE}"

    # mutations_sync = 2 so the entry is written and attributed before query_log is read.
    $CLICKHOUSE_CLIENT -m -q "
        create table local_${suffix} (id UInt64, v UInt64) engine = MergeTree order by id
        settings ${common};
        create table objstore_${suffix} (id UInt64, v UInt64) engine = MergeTree order by id
        settings disk = disk(name = '${disk_name}', type = object_storage,
                             object_storage_type = local_blob_storage, path = '${disk_name}/'),
                 ${common};
        insert into local_${suffix} select number, number from numbers(500);
        insert into objstore_${suffix} select number, number from numbers(500);
        alter table local_${suffix} update v = v + 1 where id < 10
        settings mutations_sync = 2, log_comment = 'c-local-${suffix}';
        alter table objstore_${suffix} update v = v + 1 where id < 10
        settings mutations_sync = 2, log_comment = 'c-objstore-${suffix}';
    "
}

# With fsync_after_insert an object-storage disk must sync the local metadata files that commit the
# part on top of its blobs, so it issues strictly more syncs than a local disk holding the same table.
run_case wide 1
run_case compact 1
# Without the setting neither disk forces any sync.
run_case wide 0
run_case compact 0
# With min_rows_to_fsync_after_merge the merged part's local metadata must be synced too, so the
# object-storage disk again issues strictly more syncs than the local disk for the same merge.
run_merge_case 1
# With both merge thresholds at 0 neither disk syncs anything during the merge.
run_merge_case 0
run_alter_case

CASES="wide_1 compact_1 wide_0 compact_0 merge_1 merge_0 alter"

# A row missing from query_log would be read as a sync count of 0, which no assertion here can tell
# from a real 0, so all 14 must be present. The 'rows-' rows share the shape to ride the same pass.
read_all() {
    local sel=""
    for c in $CASES; do
        sel+="select 'rows-${c}' as k, count() as a, sum(id) as b from objstore_${c} union all "
    done
    $CLICKHOUSE_CLIENT -m -q "
        system flush logs query_log;
        select k, a, b from (
            select log_comment as k,
                   argMax(ProfileEvents['FileSync'], event_time_microseconds) as a,
                   argMax(ProfileEvents['DirectorySync'], event_time_microseconds) as b
            from system.query_log
            where current_database = currentDatabase() and type = 'QueryFinish'
              and log_comment like 'c-%'
            group by log_comment
            union all
            ${sel}
            select 'guard', throwIf(count() != 14, 'missing query_log rows'), 0
            from system.query_log
            where current_database = currentDatabase() and type = 'QueryFinish'
              and log_comment like 'c-%'
        )
        format TSV;
    "
}

declare -A FS DS ROWS SUM
while IFS=$'\t' read -r k a b; do
    case "$k" in
        c-local-*)    FS[local-${k#c-local-}]=$a;    DS[local-${k#c-local-}]=$b ;;
        c-objstore-*) FS[objstore-${k#c-objstore-}]=$a; DS[objstore-${k#c-objstore-}]=$b ;;
        rows-*)       ROWS[${k#rows-}]=$a;           SUM[${k#rows-}]=$b ;;
    esac
done < <(read_all)

# args: suffix, label, arm (on|off)
report() {
    local suffix=$1 label=$2 arm=$3
    local lf=${FS[local-$suffix]} of=${FS[objstore-$suffix]} od=${DS[objstore-$suffix]}
    if [[ "$arm" == "on" ]]; then
        echo "${label}: local synced=$((lf > 0)) objstore syncs more=$((of > lf)) DirectorySync=$((od))"
    else
        echo "${label}: local FileSync=$((lf)) objstore FileSync=$((of)) DirectorySync=$((od))"
    fi
    printf '%s\t%s\n' "${ROWS[$suffix]}" "${SUM[$suffix]}"
}

# The merge and alter cases print no DirectorySync column, so they format their own line.
report_nodir() {
    local suffix=$1 label=$2 arm=$3
    local lf=${FS[local-$suffix]} of=${FS[objstore-$suffix]}
    if [[ "$arm" == "on" ]]; then
        echo "${label}: local synced=$((lf > 0)) objstore syncs more=$((of > lf))"
    else
        echo "${label}: local FileSync=$((lf)) objstore FileSync=$((of))"
    fi
    printf '%s\t%s\n' "${ROWS[$suffix]}" "${SUM[$suffix]}"
}

report       wide_1    "wide, fsync on"     on
report       compact_1 "compact, fsync on"  on
report       wide_0    "wide, fsync off"    off
report       compact_0 "compact, fsync off" off
report_nodir merge_1   "merge, fsync on"    on
report_nodir merge_0   "merge, fsync off"   off
report_nodir alter     "alter, fsync on"    on

for c in $CASES; do
    echo "drop table local_${c}; drop table objstore_${c};"
done | $CLICKHOUSE_CLIENT -m
