#!/usr/bin/env bash
# Tags: no-object-storage, no-random-merge-tree-settings
# Tag no-object-storage: s3 does not have fsync
# add_minmax_index_for_numeric_columns=0: More files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists data_fsync_pe;

    create table data_fsync_pe (key Int) engine=MergeTree()
    order by key
    settings
        min_rows_for_wide_part = 2,
        fsync_after_insert = 1,
        fsync_part_directory = 1,
        ratio_of_defaults_for_sparse_serialization = 1,
        serialization_info_version = 'basic',
        write_marks_for_substreams_in_compact_parts = 1,
        auto_statistics_types = '',
        add_minmax_index_for_numeric_columns=0;

    -- Keep every inserted part active (the retry loop below can create several) so the
    -- part selected for the ATTACH check further down cannot be replaced by a merge.
    system stop merges data_fsync_pe;
"

ret=1
# Retry in case of fsync/fdatasync was too fast
# (FileSyncElapsedMicroseconds/DirectorySyncElapsedMicroseconds was 0)
for i in {1..100}; do
    query_id="insert-$i-$CLICKHOUSE_DATABASE"

    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "insert into data_fsync_pe values (1)"

    read -r FileSync FileOpen DirectorySync FileSyncElapsedMicroseconds DirectorySyncElapsedMicroseconds <<<"$(
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs query_log;

        select
            ProfileEvents['FileSync'],
            ProfileEvents['FileOpen'],
            ProfileEvents['DirectorySync'],
            ProfileEvents['FileSyncElapsedMicroseconds']>0,
            ProfileEvents['DirectorySyncElapsedMicroseconds']>0
        from system.query_log
        where
            event_date >= yesterday() AND event_time >= now() - 600 and
            current_database = currentDatabase() and
            query_id = {query_id:String} and
            type = 'QueryFinish';
    ")"

    # Non retriable errors
    if [[ $FileSync -ne 9 ]]; then
        echo "FileSync: $FileSync != 8" >&2
        exit 2
    fi
    # Check that all files was synced
    if [[ $FileSync -ne $FileOpen ]]; then
        echo "$FileSync (FileSync) != $FileOpen (FileOpen)" >&2
        exit 3
    fi
    # With fsync_part_directory=1 an insert now fsyncs 3 directories: the 2 for the part
    # directory itself (write + tmp->final rename) plus its parent directory, so the rename
    # is itself crash-durable (the parent holds the part's new dentry).
    if [[ $DirectorySync -ne 3 ]]; then
        echo "DirectorySync: $DirectorySync != 3" >&2
        exit 4
    fi

    # Retriable errors
    if [[ $FileSyncElapsedMicroseconds -eq 0 ]]; then
        continue
    fi
    if [[ $DirectorySyncElapsedMicroseconds -eq 0 ]]; then
        continue
    fi

    # Everything is OK
    ret=0
    break
done

# Cross-parent rename: ATTACH PART commits a part from detached/attaching_* to the table root,
# so the source and destination parents differ. With fsync_part_directory=1 this fsyncs the moved
# part directory plus BOTH distinct parents (3), which guards the source-parent fsync that a
# same-parent insert does not exercise.
if [[ $ret -eq 0 ]]; then
    # Unique per run: a fixed query_id would match older rows of a rerun against the same
    # server, and the multi-line result then breaks the numeric comparison below.
    attach_query_id="attach-$CLICKHOUSE_DATABASE-$(random_str 10)"
    part_name=$($CLICKHOUSE_CLIENT -q "select name from system.parts where table='data_fsync_pe' and active and database=currentDatabase() order by name limit 1")
    $CLICKHOUSE_CLIENT -q "alter table data_fsync_pe detach part '$part_name'"
    $CLICKHOUSE_CLIENT --query_id "$attach_query_id" -q "alter table data_fsync_pe attach part '$part_name'"
    AttachDirectorySync=$($CLICKHOUSE_CLIENT -m --param_query_id "$attach_query_id" -q "
        system flush logs query_log;
        select ProfileEvents['DirectorySync']
        from system.query_log
        where
            event_date >= yesterday() AND event_time >= now() - 600 and
            current_database = currentDatabase() and
            query_id = {query_id:String} and
            type = 'QueryFinish'
        order by event_time_microseconds desc
        limit 1;
    ")
    if [[ $AttachDirectorySync -ne 3 ]]; then
        echo "ATTACH PART DirectorySync: $AttachDirectorySync != 3" >&2
        ret=5
    fi
fi

$CLICKHOUSE_CLIENT -q "drop table data_fsync_pe"

exit $ret
