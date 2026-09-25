#!/usr/bin/env bash
# Tags: atomic-database, memory-engine

# `SYSTEM REFRESH VIEW` on a view stopped or paused on this replica runs right away and once per call, and
# `SYSTEM WAIT VIEW` on a stopped view reports a failed one: https://github.com/ClickHouse/ClickHouse/issues/108352

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"

wait_status() {
    local view_name=$1
    local expected=$2
    while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = '$view_name' -- $LINENO" | xargs`" != "$expected" ]
    do
        sleep 0.1
    done
}

# Waits until the refresh has read a row, so that a cancellation
# issued right after has a concrete pipeline to interrupt.
wait_running_with_progress() {
    local view_name=$1
    while [ "`$CLICKHOUSE_CLIENT -q "select status = 'Running' and read_rows > 0 from refreshes where view = '$view_name' -- $LINENO" | xargs`" != '1' ]
    do
        sleep 0.1
    done
}

# 1. `SYSTEM REFRESH` on a stopped or paused view runs right away, once per request,
# and the view goes back to `Disabled` afterwards, like `REFRESH` on a stopped streaming table.

# `APPEND`, so that the number of rows counts the refreshes that ran.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src values (44);
    create materialized view d refresh every 1 year append (x Int64) engine Memory empty as
        select x from src;
    system stop d;"

wait_status d Disabled

$CLICKHOUSE_CLIENT -q "
    system refresh d;
    system wait view d;
    select '<1: refresh while stopped runs now>', count() from d;"
wait_status d Disabled

$CLICKHOUSE_CLIENT -q "
    system refresh view d;
    system refresh view d;
    system refresh view d;
    system wait view d;
    select '<1: three refreshes run three times>', count() from d;"
wait_status d Disabled

$CLICKHOUSE_CLIENT -q "
    system start d;
    system pause d;"
wait_status d Disabled
$CLICKHOUSE_CLIENT -q "
    system refresh d;
    system wait view d;
    select '<1: refresh while paused runs now>', count() from d;"
wait_status d Disabled

$CLICKHOUSE_CLIENT -q "
    system start d;
    system wait view d;
    select '<1: nothing pending after start>', (select count() from d), (select status from refreshes where view = 'd');
    drop table d;
    drop table src;"

# 2. `SYSTEM WAIT VIEW` reports a failed out-of-schedule refresh on a stopped view,
# the same as on a running view.

$CLICKHOUSE_CLIENT -q "
    create materialized view f refresh every 1 year (x Int64) engine Memory empty as
        select throwIf(1, 'boom')::Int64 as x;
    system stop view f;"

wait_status f Disabled
$CLICKHOUSE_CLIENT -q "
    system refresh view f;
    system wait view f; -- { serverError REFRESH_FAILED }"

wait_status f Disabled
$CLICKHOUSE_CLIENT -q "
    select '<2: failed refresh while stopped is reported>', exception like '%boom%', status from refreshes where view = 'f';
    drop table f;"

# 3. `SYSTEM STOP VIEW` cancels the running refresh only: requests queued behind it still run.

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size = 1;
    create materialized view e refresh every 1 year append (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh e;"

wait_running_with_progress e

$CLICKHOUSE_CLIENT -q "
    system refresh view e;
    system refresh view e;
    system stop view e;
    system wait view e;"

wait_status e Disabled
$CLICKHOUSE_CLIENT -q "
    select '<3: stop cancels only the running refresh>', (select count() from e), (select status from refreshes where view = 'e');
    drop table e;
    drop table src;"

# 4. `SYSTEM WAIT VIEW` stays silent about a refresh that `SYSTEM STOP VIEW` interrupted
# (error `cancelled`), while still reporting a genuine failure (2).

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size = 1;
    create materialized view g refresh every 1 year (x Int64) engine MergeTree order by x empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh g;"

wait_running_with_progress g

$CLICKHOUSE_CLIENT -q "
    system stop view g;
    system wait view g;
    select '<4: wait is silent about a cancelled refresh>', status from refreshes where view = 'g';
    drop table g;
    drop table src;"

# 5. `SYSTEM WAIT VIEW` does not report a scheduled refresh that failed before the view was
# stopped; only a failed out-of-schedule refresh (2) is reported on a stopped view.

$CLICKHOUSE_CLIENT -q "
    create materialized view sv refresh every 1 year settings refresh_retries = 0 (x Int64) engine Memory as
        select throwIf(1, 'scheduled-boom')::Int64 as x;"

# The initial (scheduled, not out-of-schedule) refresh fails; with no retries the view settles `Scheduled`.
wait_status sv Scheduled

$CLICKHOUSE_CLIENT -q "
    system stop view sv;
    system wait view sv;
    select '<5: stale scheduled failure not reported after stop>', (select exception like '%scheduled-boom%' from refreshes where view = 'sv'), (select status from refreshes where view = 'sv');
    drop table sv;"

# 6. `SYSTEM WAIT VIEW` is silent after `SYSTEM CANCEL VIEW` interrupted a refresh (the view
# stays enabled), matching `SYSTEM STOP VIEW` (4).

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size = 1;
    create materialized view cw refresh every 1 year (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh cw;"

wait_running_with_progress cw

$CLICKHOUSE_CLIENT -q "
    system cancel cw;
    system wait view cw;
    select '<6: wait is silent after cancel>', status != 'Disabled' from refreshes where view = 'cw';
    drop table cw;
    drop table src;"

# 7. `SYSTEM WAIT VIEW` reports a failed `SYSTEM REFRESH VIEW` even when the view is stopped
# only after the failure.

$CLICKHOUSE_CLIENT -q "
    create materialized view fs refresh every 1 year (x Int64) engine Memory empty as
        select throwIf(1, 'late-boom')::Int64 as x;
    system refresh view fs;"

$CLICKHOUSE_CLIENT -q "system wait view fs; -- { serverError REFRESH_FAILED }"

$CLICKHOUSE_CLIENT -q "system stop view fs;"
wait_status fs Disabled

$CLICKHOUSE_CLIENT -q "system wait view fs; -- { serverError REFRESH_FAILED }"

$CLICKHOUSE_CLIENT -q "
    select '<7: failure is reported after a later stop>', exception like '%late-boom%', status
        from refreshes where view = 'fs';
    drop table fs;"
