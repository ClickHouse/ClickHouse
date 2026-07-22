#!/usr/bin/env bash
# Tags: no-object-storage
# Tag no-object-storage: freeze on object storage does not fsync local directories

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ALTER TABLE ... FREEZE must fsync the snapshot's directories when fsync_part_directory is set,
# otherwise a power loss right after the acknowledgement erases the hardlinked snapshot (only
# shadow/increment.txt survived before the fix). We observe the directory fsyncs issued by the
# FREEZE query via ProfileEvents['DirectorySync'] in query_log.
#
# The counts assert both halves of the fix are exercised, without hardcoding the exact snapshot
# path depth (which varies by database engine): the ancestor chain up to the disk root (so the
# plain-table count is well above 1) and the recursive part subtree (so a table with a projection
# syncs exactly one more directory -- its <name>.proj subdir -- than the same table without one).

# $1 = fsync_part_directory, $2 = "proj" to add a projection. Prints the FREEZE's DirectorySync.
run_freeze() {
    local fpd=$1 proj=$2
    local tag="${CLICKHOUSE_TEST_UNIQUE_NAME}_${fpd}_${proj}_${RANDOM}${RANDOM}"
    local projection=""
    [[ "$proj" == "proj" ]] && projection=", PROJECTION p (SELECT v, count() GROUP BY v)"
    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists freeze_fsync;
        create table freeze_fsync (id UInt64, v UInt64${projection}) engine=MergeTree order by id
            settings fsync_part_directory = ${fpd};
        insert into freeze_fsync select number, number % 10 from numbers(1000);
    "
    $CLICKHOUSE_CLIENT --query_id "$tag" -q "alter table freeze_fsync freeze with name '${tag}'"
    $CLICKHOUSE_CLIENT -q "system flush logs query_log"
    $CLICKHOUSE_CLIENT --param_query_id "$tag" -q "
        select max(ProfileEvents['DirectorySync'])
        from system.query_log
        where event_date >= yesterday() and event_time >= now() - 600
          and current_database = currentDatabase()
          and query_id = {query_id:String}
          and type = 'QueryFinish';
    "
    $CLICKHOUSE_CLIENT -q "drop table freeze_fsync"
}

on_plain=$(run_freeze 1 plain)
on_proj=$(run_freeze 1 proj)
off_plain=$(run_freeze 0 plain)

# With the setting on, the FREEZE must sync the whole ancestor chain up to the disk root, so the
# count is well above just the leaf part directory.
if [[ "$on_plain" -ge 4 ]]; then
    echo "on: ancestor chain synced"
else
    echo "on: ancestor chain synced FAILED (DirectorySync=$on_plain, expected >= 4)"
fi

# The recursive part-subtree sync must also cover projection subdirectories: a table with one
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
