#!/usr/bin/env bash
# Tags: no-object-storage
# Tag no-object-storage: detach on object storage does not fsync local directories

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ALTER TABLE ... DETACH PARTITION must fsync the detached/ clone's directories when
# fsync_part_directory is set, otherwise a power loss right after the acknowledgement can erase the
# clone (issue #111382): the covering empty part that removes the rows from the active set IS
# fsynced, so on btrfs the un-synced clone dentries roll back and the only copy of the detached
# data is destroyed. We observe the directory fsyncs issued by the DETACH query via
# ProfileEvents['DirectorySync'] in query_log.
#
# The counts assert both halves of the clone durability are exercised, without hardcoding the exact
# path depth (which varies by database engine): the ancestor chain up to the disk root (so the
# plain-table count is well above the leaf part directory) and the recursive clone subtree (so a
# table with a projection syncs exactly one more directory -- its <name>.proj subdir -- than the
# same table without one).

# $1 = fsync_part_directory, $2 = "proj" to add a projection. Prints the DETACH's DirectorySync.
run_detach() {
    local fpd=$1 proj=$2
    local qid="${CLICKHOUSE_TEST_UNIQUE_NAME}_${fpd}_${proj}_${RANDOM}${RANDOM}"
    local projection=""
    [[ "$proj" == "proj" ]] && projection=", PROJECTION p (SELECT v, count() GROUP BY v)"
    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists detach_fsync;
        create table detach_fsync (id UInt64, v UInt64${projection}) engine=MergeTree order by id
            settings fsync_part_directory = ${fpd}, storage_policy = 'default';
        system stop merges detach_fsync;
        insert into detach_fsync select number, number % 10 from numbers(1000);
    "
    $CLICKHOUSE_CLIENT --query_id "$qid" -q "alter table detach_fsync detach partition tuple()"
    $CLICKHOUSE_CLIENT -q "system flush logs query_log"
    $CLICKHOUSE_CLIENT --param_query_id "$qid" -q "
        select max(ProfileEvents['DirectorySync'])
        from system.query_log
        where event_date >= yesterday() and event_time >= now() - 600
          and current_database = currentDatabase()
          and query_id = {query_id:String}
          and type = 'QueryFinish';
    "
    # The detached clone must be present and fully re-attachable (data preserved).
    $CLICKHOUSE_CLIENT -q "alter table detach_fsync attach partition tuple()"
    $CLICKHOUSE_CLIENT -q "select count() from detach_fsync"
    $CLICKHOUSE_CLIENT -q "drop table detach_fsync"
}

read -r on_plain on_plain_rows < <(run_detach 1 plain | tr '\n' ' ')
read -r on_proj on_proj_rows < <(run_detach 1 proj | tr '\n' ' ')
read -r off_plain off_plain_rows < <(run_detach 0 plain | tr '\n' ' ')

# With the setting on, the DETACH must sync the clone plus the whole ancestor chain up to the disk
# root, so the count is well above just the leaf part directory.
if [[ "$on_plain" -ge 4 ]]; then
    echo "on: ancestor chain synced"
else
    echo "on: ancestor chain synced FAILED (DirectorySync=$on_plain, expected >= 4)"
fi

# The recursive clone-subtree sync must also cover projection subdirectories: a table with one
# projection syncs exactly one more directory (its <name>.proj) than the same table without one.
if [[ "$on_proj" -eq $((on_plain + 1)) ]]; then
    echo "proj: subtree synced"
else
    echo "proj: subtree synced FAILED (plain=$on_plain proj=$on_proj, expected proj = plain + 1)"
fi

# With fsync_part_directory = 0 (the default) behavior is unchanged: no directory fsync.
if [[ "$off_plain" -eq 0 ]]; then
    echo "off: DirectorySync = 0"
else
    echo "off: DirectorySync = $off_plain (expected 0)"
fi

# The detached data must survive and re-attach in every case (fsync on and off).
if [[ "$on_plain_rows" -eq 1000 && "$on_proj_rows" -eq 1000 && "$off_plain_rows" -eq 1000 ]]; then
    echo "reattach: data preserved"
else
    echo "reattach: data preserved FAILED (on_plain=$on_plain_rows on_proj=$on_proj_rows off_plain=$off_plain_rows, expected 1000 each)"
fi
