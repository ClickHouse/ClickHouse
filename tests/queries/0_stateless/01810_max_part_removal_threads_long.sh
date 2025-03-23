#!/usr/bin/env bash
# Tags: long, no-object-storage
# Because parallel parts removal disabled for s3 storage

# NOTE: this done as not .sql since we need to Ordinary database
# (to account threads in query_log for DROP TABLE query)
# and we can do it compatible with parallel run only in .sh
# (via $CLICKHOUSE_DATABASE)

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The number of threads removing data parts should be between 1 and 129.
# Because max_parts_cleaning_thread_pool_size is 128 by default

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -m -q "create database ordinary_$CLICKHOUSE_DATABASE engine=Ordinary"

# MergeTree
$CLICKHOUSE_CLIENT -m -q """
    use ordinary_$CLICKHOUSE_DATABASE;
    drop table if exists data_01810;

    create table data_01810 (key Int)
    Engine=MergeTree()
    order by key
    partition by key%100
    settings concurrent_part_removal_threshold=99, min_bytes_for_wide_part=0;

    insert into data_01810 select * from numbers(100);
    drop table data_01810 settings log_queries=1;
    system flush logs;

    -- sometimes the same thread can be used to remove part, due to ThreadPool,
    -- hence we cannot compare strictly.
    select throwIf(not(length(thread_ids) between 1 and 129))
    from system.query_log
    where
        event_date >= yesterday() and
        current_database = currentDatabase() and
        query = 'drop table data_01810 settings log_queries=1;' and
        type = 'QueryFinish'
    format Null;
"""

# ReplicatedMergeTree
$CLICKHOUSE_CLIENT -m -q """
    use ordinary_$CLICKHOUSE_DATABASE;
    drop table if exists rep_data_01810;

    create table rep_data_01810 (key Int)
    Engine=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rep_data_01810', '1')
    order by key
    partition by key%100
    settings concurrent_part_removal_threshold=99, min_bytes_for_wide_part=0;

    SET insert_keeper_max_retries=1000;
    SET insert_keeper_retry_max_backoff_ms=10;

    insert into rep_data_01810 select * from numbers(100);
    drop table rep_data_01810 settings log_queries=1;
    system flush logs;

    -- sometimes the same thread can be used to remove part, due to ThreadPool,
    -- hence we cannot compare strictly.
    select throwIf(not(length(thread_ids) between 1 and 129))
    from system.query_log
    where
        event_date >= yesterday() and
        current_database = currentDatabase() and
        query = 'drop table rep_data_01810 settings log_queries=1;' and
        type = 'QueryFinish'
    format Null;
"""

$CLICKHOUSE_CLIENT -m -q "drop database ordinary_$CLICKHOUSE_DATABASE"
