#!/usr/bin/env bash
# Tags: no-object-storage
# Tag no-object-storage: object storage disks do not fsync local directories.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111349
# The DirectorySync ProfileEvent is used as a durability proxy for the detached/
# directory fsync (instead of simulating a power loss).
# storage_policy = 'default' pins the table to local disk: the stress test does not pass
# --s3-storage (so no-object-storage does not skip it) yet may make an object storage policy
# the MergeTree default, where the directory fsync is a no-op.

run_case() {
    local fsync=$1
    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists t_dd;
        create table t_dd (x UInt32) engine = MergeTree order by x settings fsync_part_directory = $fsync, storage_policy = 'default';
        insert into t_dd values (1);
        alter table t_dd detach partition tuple();
    "
    local part
    part=$($CLICKHOUSE_CLIENT -q "select name from system.detached_parts where database = currentDatabase() and table = 't_dd' limit 1")
    local query_id="drop-detached-fsync${fsync}-$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "alter table t_dd drop detached part '$part' settings allow_drop_detached = 1"
    $CLICKHOUSE_CLIENT -m --param_query_id "$query_id" -q "
        system flush logs query_log;
        select ProfileEvents['DirectorySync'] >= 1
        from system.query_log
        where event_date >= yesterday() and current_database = currentDatabase()
          and query_id = {query_id:String} and type = 'QueryFinish'
        order by event_time_microseconds desc limit 1;
    "
    # The part must be gone regardless of the setting.
    $CLICKHOUSE_CLIENT -q "select count() from system.detached_parts where database = currentDatabase() and table = 't_dd'"
    $CLICKHOUSE_CLIENT -q "drop table t_dd"
}

echo "fsync_part_directory = 1: DirectorySync>=1, part removed"
run_case 1
echo "fsync_part_directory = 0: DirectorySync=0 (setting honored), part removed"
run_case 0
