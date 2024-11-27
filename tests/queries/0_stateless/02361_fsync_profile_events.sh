#!/usr/bin/env bash
# Tags: no-object-storage, no-random-merge-tree-settings
# Tag no-object-storage: s3 does not have fsync

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
        ratio_of_defaults_for_sparse_serialization = 1;
"

ret=1
# Retry in case of fsync/fdatasync was too fast
# (FileSyncElapsedMicroseconds/DirectorySyncElapsedMicroseconds was 0)
for i in {1..100}; do
    query_id="insert-$i-$CLICKHOUSE_DATABASE"

    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "insert into data_fsync_pe values (1)"

    read -r FileSync FileOpen DirectorySync FileSyncElapsedMicroseconds DirectorySyncElapsedMicroseconds <<<"$(
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs;

        select
            ProfileEvents['FileSync'],
            ProfileEvents['FileOpen'],
            ProfileEvents['DirectorySync'],
            ProfileEvents['FileSyncElapsedMicroseconds']>0,
            ProfileEvents['DirectorySyncElapsedMicroseconds']>0
        from system.query_log
        where
            event_date >= yesterday() and
            current_database = currentDatabase() and
            query_id = {query_id:String} and
            type = 'QueryFinish';
    ")"

    # Non retriable errors
    if [[ $FileSync -ne 8 ]]; then
        echo "FileSync: $FileSync != 8" >&2
        exit 2
    fi
    # Check that all files was synced
    if [[ $FileSync -ne $FileOpen ]]; then
        echo "$FileSync (FileSync) != $FileOpen (FileOpen)" >&2
        exit 3
    fi
    if [[ $DirectorySync -ne 2 ]]; then
        echo "DirectorySync: $DirectorySync != 2" >&2
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

$CLICKHOUSE_CLIENT -q "drop table data_fsync_pe"

exit $ret
