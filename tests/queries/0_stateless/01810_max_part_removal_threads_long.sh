#!/usr/bin/env bash

# NOTE: this done as not .sql since we need to Ordinary database
# (to account threads in query_log for DROP TABLE query)
# and we can do it compatible with parallel run only in .sh
# (via $CLICKHOUSE_DATABASE)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "create database ordinary_$CLICKHOUSE_DATABASE engine=Ordinary"

# MergeTree
$CLICKHOUSE_CLIENT -nm -q """
    use ordinary_$CLICKHOUSE_DATABASE;
    drop table if exists data_01810;
    create table data_01810 (key Int) Engine=MergeTree() order by key partition by key settings max_part_removal_threads=10, concurrent_part_removal_threshold=49;
    insert into data_01810 select * from numbers(50);
    drop table data_01810 settings log_queries=1;
    system flush logs;
    select throwIf(length(thread_ids)<50) from system.query_log where event_date = today() and current_database = currentDatabase() and query = 'drop table data_01810 settings log_queries=1;' and type = 'QueryFinish' format Null;
"""

# ReplicatedMergeTree
$CLICKHOUSE_CLIENT -nm -q """
    use ordinary_$CLICKHOUSE_DATABASE;
    drop table if exists rep_data_01810;
    create table rep_data_01810 (key Int) Engine=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rep_data_01810', '1') order by key partition by key settings max_part_removal_threads=10, concurrent_part_removal_threshold=49;
    insert into rep_data_01810 select * from numbers(50);
    drop table rep_data_01810 settings log_queries=1;
    system flush logs;
    select throwIf(length(thread_ids)<50) from system.query_log where event_date = today() and current_database = currentDatabase() and query = 'drop table rep_data_01810 settings log_queries=1;' and type = 'QueryFinish' format Null;
"""

$CLICKHOUSE_CLIENT -nm -q "drop database ordinary_$CLICKHOUSE_DATABASE"
