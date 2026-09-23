#!/usr/bin/env bash
# Tags: zookeeper

# Regression test: SYSTEM START REPLICATED VIEW did not wake the refresh task
# because the ZooKeeper children watch was not re-registered after the first fire.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT=$(echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g')
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --session_timezone Etc/UTC"

db="rdb_$CLICKHOUSE_DATABASE"

function cleanup()
{
    $CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "drop database if exists $db" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -nq "
    create database $db engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's1', 'r1');
    create view ${db}.refreshes as
        select * from system.view_refreshes where database = '$db' order by view;
    create materialized view ${db}.rmv
        refresh after 1 second append
        (x Int64) engine MergeTree order by x
        empty
        as select 1 as x;
"

# Wait for the first refresh to succeed.
for _ in $(seq 1 60); do
    if [ "$($CLICKHOUSE_CLIENT -q "select last_success_time is null from ${db}.refreshes -- $LINENO" | xargs)" = '0' ]; then
        break
    fi
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "select '<1: running>', status != 'Disabled' from ${db}.refreshes"

# Stop the view globally via Keeper.
$CLICKHOUSE_CLIENT -q "system stop replicated view ${db}.rmv"
for _ in $(seq 1 60); do
    if [ "$($CLICKHOUSE_CLIENT -q "select status from ${db}.refreshes -- $LINENO" | xargs)" = 'Disabled' ]; then
        break
    fi
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "select '<2: stopped>', status from ${db}.refreshes"

# Remember the current row count.
cnt_before=$($CLICKHOUSE_CLIENT -q "select count() from ${db}.rmv")

# Start the view back up.
$CLICKHOUSE_CLIENT -q "system start replicated view ${db}.rmv"

# The bug: the view stayed Disabled forever here because the children watch
# was not re-registered after the stop event consumed it.
# Wait for the view to leave Disabled state and complete at least one more refresh.
# Budget must cover resume detection plus a full refresh run, so it is not shorter
# than the initial-refresh wait above.
for _ in $(seq 1 120); do
    cnt_after=$($CLICKHOUSE_CLIENT -q "select count() from ${db}.rmv")
    if [ "$cnt_after" -gt "$cnt_before" ]; then
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "select '<3: restarted>', status != 'Disabled' from ${db}.refreshes"
$CLICKHOUSE_CLIENT -q "select '<4: new rows>', count() > $cnt_before from ${db}.rmv"

# ---------------------------------------------------------------------------
# A refresh requested while the view is stopped cluster-wide stays deferred until
# `SYSTEM START REPLICATED VIEW`, and then runs. This is the one stop that
# `SYSTEM REFRESH VIEW` does not override - a local `SYSTEM STOP VIEW` it does.
# ---------------------------------------------------------------------------

# `APPEND`, so the row count is the number of refreshes that ran, and `EVERY 1 YEAR` with
# `EMPTY` so that nothing refreshes on its own while the test runs.
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -nq "
    create materialized view ${db}.rmv2
        refresh every 1 year append
        (x Int64) engine MergeTree order by x
        empty
        as select 2 as x;
    system stop replicated view ${db}.rmv2;
"
for _ in $(seq 1 60); do
    if [ "$($CLICKHOUSE_CLIENT -q "select status from ${db}.refreshes where view = 'rmv2' -- $LINENO" | xargs)" = 'Disabled' ]; then
        break
    fi
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "select '<5: stopped cluster-wide>', status from ${db}.refreshes where view = 'rmv2'"

# The request is accepted, but must not run. The loop gives it a window to run in and
# breaks early if it does, so a regression here fails instead of passing slowly.
$CLICKHOUSE_CLIENT -q "system refresh view ${db}.rmv2"
for _ in $(seq 1 6); do
    if [ "$($CLICKHOUSE_CLIENT -q "select count() from ${db}.rmv2 -- $LINENO" | xargs)" != '0' ]; then
        break
    fi
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "select '<6: refresh deferred>', (select count() from ${db}.rmv2), (select status from ${db}.refreshes where view = 'rmv2')"

# Resuming runs the refresh that was requested while the view was stopped.
$CLICKHOUSE_CLIENT -q "system start replicated view ${db}.rmv2"
for _ in $(seq 1 120); do
    if [ "$($CLICKHOUSE_CLIENT -q "select count() from ${db}.rmv2 -- $LINENO" | xargs)" = '1' ]; then
        break
    fi
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "select '<7: deferred refresh runs after resume>', count() from ${db}.rmv2"
