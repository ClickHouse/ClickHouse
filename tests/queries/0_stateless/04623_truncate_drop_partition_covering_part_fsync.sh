#!/usr/bin/env bash
# Tags: no-object-storage
# Tag no-object-storage: these ops fsync local directories; on object storage the guard is a no-op

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# TRUNCATE / ALTER DROP PARTITION / ALTER DROP PART commit one empty covering part per removed part
# and then unlink the covered parts before acknowledging. The covering part is the operation's only
# on-disk commit record, so it must be durable before the ack -- otherwise a power loss right after
# the acknowledgement rolls back the covering-part rename and the covered parts (still on disk)
# reappear, resurrecting acked-deleted data (issue #111348). Unlike a plain INSERT these ops also
# set remove_time=0 on the covered parts, bypassing the old_parts_lifetime retention that would
# otherwise keep them recoverable, so the durability must not depend on the default-off
# fsync_after_insert / fsync_part_directory settings.
#
# We assert the durability at DEFAULT settings via ProfileEvents in query_log:
#  - FileSync > 0: the covering part's files were fsync'd (createEmptyPart force_sync).
#  - DirectorySync == (one per covered part, for each covering part's own directory) + 1 for the
#    single parent table directory. Asserting the exact count (rather than just >= 1) fails if
#    either the per-part directory sync or the essential parent-directory sync is removed.

# $1 = label, $2 = expected covered/covering part count, remaining args = the DDL statement.
run_and_report() {
    local label=$1 covered=$2
    shift 2
    # A fresh query_id per invocation so the query_log lookup can never pick up a same-named row
    # from an earlier run of this test on the same server.
    local qid="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}_${RANDOM}${RANDOM}"
    $CLICKHOUSE_CLIENT --query_id "$qid" -q "$*"
    $CLICKHOUSE_CLIENT -q "system flush logs query_log"
    read -r dir_sync file_sync < <($CLICKHOUSE_CLIENT --param_query_id "$qid" -q "
        select max(ProfileEvents['DirectorySync']), max(ProfileEvents['FileSync'])
        from system.query_log
        where event_date >= yesterday() and event_time >= now() - 600
          and current_database = currentDatabase()
          and query_id = {query_id:String}
          and type = 'QueryFinish'
        format TSV")
    # One directory sync per covering part (its own dir) plus one for the shared parent table dir.
    local expected_dir=$(( covered + 1 ))
    if [[ "$dir_sync" == "$expected_dir" && "$file_sync" -gt 0 ]]; then
        echo "$label: durable"
    else
        echo "$label: NOT durable (DirectorySync=$dir_sync expected $expected_dir, FileSync=$file_sync expected > 0)"
    fi
}

# TRUNCATE -- two parts covered.
$CLICKHOUSE_CLIENT -m -q "
    drop table if exists t_truncate;
    create table t_truncate (a UInt64) engine=MergeTree order by a settings storage_policy='default';
    system stop merges t_truncate;
    insert into t_truncate select number from numbers(1000);
    insert into t_truncate select number from numbers(1000, 1000);
"
run_and_report "truncate" 2 "truncate table t_truncate"
echo "truncate rows: $($CLICKHOUSE_CLIENT -q "select count() from t_truncate")"
$CLICKHOUSE_CLIENT -q "drop table t_truncate"

# DROP PARTITION -- one partition (one part) covered, another retained.
$CLICKHOUSE_CLIENT -m -q "
    drop table if exists t_drop_partition;
    create table t_drop_partition (p UInt8, a UInt64) engine=MergeTree partition by p order by a settings storage_policy='default';
    system stop merges t_drop_partition;
    insert into t_drop_partition select 1, number from numbers(1000);
    insert into t_drop_partition select 2, number from numbers(1000);
"
run_and_report "drop_partition" 1 "alter table t_drop_partition drop partition 1"
echo "drop_partition rows: $($CLICKHOUSE_CLIENT -q "select count() from t_drop_partition")"
$CLICKHOUSE_CLIENT -q "drop table t_drop_partition"

# DROP PART -- one part covered.
$CLICKHOUSE_CLIENT -m -q "
    drop table if exists t_drop_part;
    create table t_drop_part (a UInt64) engine=MergeTree order by a settings storage_policy='default';
    system stop merges t_drop_part;
    insert into t_drop_part select number from numbers(1000);
"
part=$($CLICKHOUSE_CLIENT -q "select name from system.parts where database=currentDatabase() and table='t_drop_part' and active limit 1")
run_and_report "drop_part" 1 "alter table t_drop_part drop part '$part'"
echo "drop_part rows: $($CLICKHOUSE_CLIENT -q "select count() from t_drop_part")"
$CLICKHOUSE_CLIENT -q "drop table t_drop_part"

# DETACH PARTITION must NOT force the covering-part sync: the detached clone is not synced here, so
# forcing only the removal durable could lose the detached copy (that durability belongs to DETACH's
# own fix). At default settings DETACH therefore issues no directory sync.
$CLICKHOUSE_CLIENT -m -q "
    drop table if exists t_detach;
    create table t_detach (p UInt8, a UInt64) engine=MergeTree partition by p order by a settings storage_policy='default';
    system stop merges t_detach;
    insert into t_detach select 1, number from numbers(1000);
    insert into t_detach select 2, number from numbers(1000);
"
detach_qid="${CLICKHOUSE_TEST_UNIQUE_NAME}_detach_${RANDOM}${RANDOM}"
$CLICKHOUSE_CLIENT --query_id "$detach_qid" -q "alter table t_detach detach partition 1"
$CLICKHOUSE_CLIENT -q "system flush logs query_log"
detach_dir_sync=$($CLICKHOUSE_CLIENT --param_query_id "$detach_qid" -q "
    select max(ProfileEvents['DirectorySync'])
    from system.query_log
    where event_date >= yesterday() and event_time >= now() - 600
      and current_database = currentDatabase()
      and query_id = {query_id:String}
      and type = 'QueryFinish'")
if [[ "$detach_dir_sync" == "0" ]]; then
    echo "detach: not force-synced"
else
    echo "detach: unexpectedly force-synced (DirectorySync=$detach_dir_sync expected 0)"
fi
$CLICKHOUSE_CLIENT -q "drop table t_detach"
