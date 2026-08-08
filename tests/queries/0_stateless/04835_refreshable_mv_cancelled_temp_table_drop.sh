#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The setting is pinned in the view's SELECT (not the session) because that is what reaches the
# refresh context, where the drop of the temporary table runs.
$CLICKHOUSE_CLIENT -q "
    create materialized view v refresh every 1 year settings refresh_retries = 0 (x UInt64)
        engine MergeTree order by x empty as
        select number + sleepEachRow(0.05) as x from numbers(200)
        settings max_block_size = 1, max_threads = 1,
                 database_atomic_wait_for_drop_and_detach_synchronously = 1;
    system refresh view v;"

# Cancel only once the refresh has read a row, so there is a live pipeline to interrupt and the
# temporary table already exists.
for _ in {1..600}
do
    [ "`$CLICKHOUSE_CLIENT -q "select status = 'Running' and read_rows > 0 from system.view_refreshes where database = currentDatabase() and view = 'v'"`" = 1 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "system cancel view v"

for _ in {1..600}
do
    [ "`$CLICKHOUSE_CLIENT -q "select status from system.view_refreshes where database = currentDatabase() and view = 'v'"`" = Scheduled ] && break
    sleep 0.1
done

# The temporary table is dropped asynchronously, so give the background drop a moment to retire it
# before asserting that nothing is left behind.
for _ in {1..600}
do
    [ "`$CLICKHOUSE_CLIENT -q "select count() from system.tables where database = currentDatabase() and name like '.tmp.inner_id.%'"`" = 0 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "system flush logs text_log"

# 1: the cancellation must not be reported as a failure to drop the temporary table.
# 2: the temporary table must really be gone, so that 1 cannot pass by merely silencing the message.
$CLICKHOUSE_CLIENT -q "
    select 'no spurious error', count() from system.text_log
        where event_date >= yesterday() and event_time >= now() - interval 10 minute
            and logger_name = 'StorageMaterializedView'
            and message like '%Failed to drop temporary table after refresh%'
            and message like '%' || currentDatabase() || '.v:%'
        settings max_rows_to_read = 0;
    select 'temp table dropped', count() from system.tables
        where database = currentDatabase() and name like '.tmp.inner_id.%';
    select 'refresh was cancelled', exception = 'cancelled' from system.view_refreshes
        where database = currentDatabase() and view = 'v';"
