#!/usr/bin/env bash
# Tags: atomic-database, memory-engine, no-parallel

# Uses `SYSTEM ... ALL BACKGROUND` commands, which affect all refreshable views
# on the server, so it cannot run concurrently with other tests that create RMV
# https://github.com/ClickHouse/ClickHouse/issues/102707

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Set session timezone to UTC to make all DateTime formatting and parsing use UTC, because refresh
# scheduling is done in UTC.
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --allow_materialized_view_with_bad_select=0 --session_timezone Etc/UTC"`"

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"

# Helper: wait until the view's status matches the expected one.
wait_status() {
    local view_name=$1
    local expected=$2
    while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = '$view_name' -- $LINENO" | xargs`" != "$expected" ]
    do
        sleep 0.1
    done
}

# Helper: wait until the refresh is running AND has read at least one row, so that a cancellation
# issued right after has a concrete pipeline to interrupt (see 04105_system_pause_view).
wait_running_with_progress() {
    local view_name=$1
    while [ "`$CLICKHOUSE_CLIENT -q "select status = 'Running' and read_rows > 0 from refreshes where view = '$view_name' -- $LINENO" | xargs`" != '1' ]
    do
        sleep 0.1
    done
}

# ---------------------------------------------------------------------------
# Test 1: SYSTEM PAUSE <view> does NOT interrupt the current refresh.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(5) settings max_block_size=1;
    create materialized view p refresh every 1 year (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh p;"

wait_status p Running

# Pause while the refresh is running. This should NOT cancel the ongoing refresh.
$CLICKHOUSE_CLIENT -q "system pause p;"

# The currently running refresh must complete successfully.
$CLICKHOUSE_CLIENT -q "system wait view p;"

wait_status p Disabled

$CLICKHOUSE_CLIENT -q "
    select '<1: pause allows current refresh to complete>',
        (select count() from p),
        (select exception = '' from refreshes where view = 'p'),
        (select last_success_time is not null from refreshes where view = 'p');"

$CLICKHOUSE_CLIENT -q "
    system start view p;
    drop table p;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 2: SYSTEM STOP <view> DOES interrupt the current refresh.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size=1;
    create materialized view s refresh every 1 year (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh s;"

wait_running_with_progress s

$CLICKHOUSE_CLIENT -q "system stop s;"

wait_status s Disabled

$CLICKHOUSE_CLIENT -q "
    select '<2: stop cancels the refresh>',
        (select count() from s),
        (select exception != '' from refreshes where view = 's');"

# SYSTEM START resumes a stopped view (the view leaves the Disabled state).
$CLICKHOUSE_CLIENT -q "system start s;"
while [ "`$CLICKHOUSE_CLIENT -q "select status = 'Disabled' from refreshes where view = 's' -- $LINENO" | xargs`" == '1' ]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    select '<2b: start resumes the stopped view>',
        (select status != 'Disabled' from refreshes where view = 's');
    system stop view s;
    drop table s;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 3: SYSTEM CANCEL <view> interrupts the current refresh but keeps the
# schedule, so the view stays enabled and re-runs.
# ---------------------------------------------------------------------------

# refresh_retries = 0 so the interrupted refresh is not retried, like in
# 02932_refreshable_materialized_views_2.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size=1;
    create materialized view c refresh every 1 year settings refresh_retries = 0 (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh c;"

wait_running_with_progress c

$CLICKHOUSE_CLIENT -q "system cancel c;"

# After cancel, the view is not disabled: the interrupted refresh leaves the view empty
# and the next refresh stays on schedule (a year away), so the status returns to Scheduled.
wait_status c Scheduled

$CLICKHOUSE_CLIENT -q "
    select '<3: cancel interrupts current refresh but keeps schedule>',
        (select count() from c),
        (select status != 'Disabled' from refreshes where view = 'c');
    system stop view c;
    drop table c;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4: SYSTEM REFRESH <view> triggers an out-of-schedule refresh.
# ---------------------------------------------------------------------------

# The view is created EMPTY with a yearly schedule, so without an explicit refresh it
# would stay empty for a year. SYSTEM REFRESH forces an out-of-schedule refresh now.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src values (43);
    create materialized view r refresh every 1 year (x Int64) engine Memory empty as
        select x from src;
    system refresh r;
    system wait view r;
    select '<4: refresh triggers out-of-schedule refresh>', * from r;
    system stop view r;
    drop table r;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4b: SYSTEM REFRESH on a STOPPED or PAUSED view runs right away, once per
# request, and the view goes back to Disabled afterwards. Same as SYSTEM REFRESH
# VIEW and as REFRESH on a stopped streaming engine table.
# ---------------------------------------------------------------------------

# APPEND, so that the number of rows counts the refreshes that ran.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src values (44);
    create materialized view d refresh every 1 year append (x Int64) engine Memory empty as
        select x from src;
    system stop d;"

wait_status d Disabled

# REFRESH while stopped runs now, SYSTEM WAIT VIEW waits for it. Then the view is stopped again.
$CLICKHOUSE_CLIENT -q "
    system refresh d;
    system wait view d;
    select '<4b: refresh while stopped runs now>', count() from d;"
wait_status d Disabled

# Several requests run that many refreshes. WAIT VIEW waits for the last of them.
$CLICKHOUSE_CLIENT -q "
    system refresh view d;
    system refresh view d;
    system refresh view d;
    system wait view d;
    select '<4b: three refreshes run three times>', count() from d;"
wait_status d Disabled

# The same while paused.
$CLICKHOUSE_CLIENT -q "
    system start d;
    system pause d;"
wait_status d Disabled
$CLICKHOUSE_CLIENT -q "
    system refresh d;
    system wait view d;
    select '<4b: refresh while paused runs now>', count() from d;"
wait_status d Disabled

# Nothing is left pending: START does not fire a deferred refresh.
$CLICKHOUSE_CLIENT -q "
    system start d;
    system wait view d;
    select '<4b: nothing pending after start>', (select count() from d), (select status from refreshes where view = 'd');
    system stop view d;
    drop table d;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4c: a REFRESH queued behind a running refresh survives a PAUSE: the
# running refresh finishes, then the queued one runs, and only then the view
# becomes Disabled.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(5) settings max_block_size = 1;
    create materialized view q refresh every 1 year append (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh q;"

wait_status q Running

# SYSTEM WAIT VIEW waits for the queued refresh as well, not only for the running one.
$CLICKHOUSE_CLIENT -q "
    system refresh q;
    system pause q;
    system wait view q;"

wait_status q Disabled
$CLICKHOUSE_CLIENT -q "
    select '<4c: queued refresh runs after pause>', (select count() from q), (select status from refreshes where view = 'q');
    system start view q;
    drop table q;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4d: SYSTEM WAIT VIEW reports a failed out-of-schedule refresh on a stopped
# view, the same as on a running view.
# ---------------------------------------------------------------------------

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
    select '<4d: failed refresh while stopped is reported>', exception like '%boom%', status from refreshes where view = 'f';
    drop table f;"

# ---------------------------------------------------------------------------
# Test 4e: SYSTEM STOP cancels the running refresh only. Requests queued behind it
# still run, like REFRESH permits on a stopped streaming engine table.
# ---------------------------------------------------------------------------

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
    select '<4e: stop cancels only the running refresh>', (select count() from e), (select status from refreshes where view = 'e');
    system start view e;
    drop table e;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4f: after CREATE OR REPLACE of a refreshable view, SYSTEM REFRESH VIEW runs
# (the replacement view's paused refresher is resumed on commit).
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src values (7);
    create materialized view rr refresh every 1 year append (x Int64) engine Memory empty as select x from src;
    create or replace materialized view rr refresh every 1 year append (x Int64) engine Memory empty as select x + 1 as x from src;
    system refresh view rr;
    system wait view rr;
    select '<4f: refresh runs after create or replace>', (select count() from rr), (select status from refreshes where view = 'rr');
    system stop view rr;
    drop table rr;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4g: SYSTEM WAIT VIEW stays silent about a refresh that SYSTEM STOP VIEW
# interrupted (error "cancelled"), while still reporting a genuine failure (4d).
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size = 1;
    create materialized view g refresh every 1 year (x Int64) engine MergeTree order by x empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh g;"

wait_running_with_progress g

# STOP cancels the running refresh; WAIT VIEW must return without error (not report "cancelled").
$CLICKHOUSE_CLIENT -q "
    system stop view g;
    system wait view g;
    select '<4g: wait is silent about a cancelled refresh>', status from refreshes where view = 'g';
    drop table g;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4h: SYSTEM WAIT VIEW does not report a scheduled-refresh failure from
# before the view was stopped; only a failed out-of-schedule refresh (4d) is
# reported on a stopped view.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create materialized view sv refresh every 1 year settings refresh_retries = 0 (x Int64) engine Memory as
        select throwIf(1, 'scheduled-boom')::Int64 as x;"

# The initial (scheduled, not out-of-schedule) refresh fails; with no retries the view settles Scheduled.
wait_status sv Scheduled

# WAIT VIEW must stay silent about that failure even after the view is stopped.
$CLICKHOUSE_CLIENT -q "
    system stop view sv;
    system wait view sv;
    select '<4h: stale scheduled failure not reported after stop>', (select exception like '%scheduled-boom%' from refreshes where view = 'sv'), (select status from refreshes where view = 'sv');
    drop table sv;"

# ---------------------------------------------------------------------------
# Test 4i: SYSTEM WAIT VIEW is silent after SYSTEM CANCEL VIEW interrupted a
# refresh (the view stays enabled), matching SYSTEM STOP VIEW (test 4g).
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size = 1;
    create materialized view cw refresh every 1 year settings refresh_retries = 0 (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh cw;"

wait_running_with_progress cw

$CLICKHOUSE_CLIENT -q "
    system cancel cw;
    system wait view cw;
    select '<4i: wait is silent after cancel>', status != 'Disabled' from refreshes where view = 'cw';
    system stop view cw;
    drop table cw;
    drop table src;"

# ---------------------------------------------------------------------------
# The SYSTEM ... ALL BACKGROUND wildcard applies the command to every table with
# background activity. Here those tables are two refreshable views; the same code
# path covers the streaming engines (Kafka, S3Queue, RabbitMQ, NATS) too.
# The views use a yearly schedule so they only refresh when explicitly told to.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src values (1);
    create materialized view va refresh every 1 year (x Int64) engine Memory empty
        as select x from src;
    create materialized view vb refresh every 1 year (x Int64) engine Memory empty
        as select x from src;"

# Test 5: SYSTEM REFRESH ALL BACKGROUND refreshes every view.
$CLICKHOUSE_CLIENT -q "
    system refresh all background;
    system wait view va;
    system wait view vb;
    select '<5: refresh all background refreshes all views>',
        (select count() from va), (select count() from vb);"

# Test 6: SYSTEM STOP ALL BACKGROUND disables every view.
$CLICKHOUSE_CLIENT -q "system stop all background;"
wait_status va Disabled
wait_status vb Disabled
$CLICKHOUSE_CLIENT -q "
    select '<6: stop all background disables all>',
        countIf(status = 'Disabled'), count()
    from refreshes where view in ('va', 'vb');
    system start all background;"
while [ "`$CLICKHOUSE_CLIENT -q "select countIf(status = 'Disabled') from refreshes where view in ('va', 'vb') -- $LINENO" | xargs`" != '0' ]
do
    sleep 0.1
done

# Test 7: SYSTEM PAUSE ALL BACKGROUND disables every view.
$CLICKHOUSE_CLIENT -q "system pause all background;"
wait_status va Disabled
wait_status vb Disabled
$CLICKHOUSE_CLIENT -q "
    select '<7: pause all background disables all>',
        countIf(status = 'Disabled'), count()
    from refreshes where view in ('va', 'vb');
    system start all background;"
while [ "`$CLICKHOUSE_CLIENT -q "select countIf(status = 'Disabled') from refreshes where view in ('va', 'vb') -- $LINENO" | xargs`" != '0' ]
do
    sleep 0.1
done

# Test 8: SYSTEM CANCEL ALL BACKGROUND interrupts current activity without disabling.
$CLICKHOUSE_CLIENT -q "
    system cancel all background;
    select '<8: cancel all background keeps views enabled>',
        countIf(status != 'Disabled'), count()
    from refreshes where view in ('va', 'vb');
    system stop view va;
    system stop view vb;
    drop table va;
    drop table vb;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 9: Access control. The engine-agnostic commands require the same
# privilege as their SYSTEM ... VIEW aliases (SYSTEM VIEWS for a view).
# ---------------------------------------------------------------------------

test_user="user_04319_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "
    create user $test_user;
    create table src (x Int64) engine Memory;
    insert into src values (1);
    create materialized view granted refresh every 1 year (x Int64) engine Memory empty as select x from src;
    create materialized view denied refresh every 1 year (x Int64) engine Memory empty as select x from src;
    system wait view granted;
    system wait view denied;
    grant system views on $CLICKHOUSE_DATABASE.granted to $test_user;"

# Every per-table verb (including START) must be denied without SYSTEM VIEWS on that
# view, and allowed on the view the user was granted.
for verb in stop start pause cancel refresh; do
    $CLICKHOUSE_CLIENT --user $test_user -q "
        system $verb $CLICKHOUSE_DATABASE.denied; -- {serverError ACCESS_DENIED}"
    $CLICKHOUSE_CLIENT --user $test_user -q "
        system $verb $CLICKHOUSE_DATABASE.granted;"
done

# A non-controllable table reports BAD_ARGUMENTS to a caller holding either child grant,
# and stays ACCESS_DENIED without any grant.
$CLICKHOUSE_CLIENT -q "create table plain_mt (x Int64) engine MergeTree order by x;"
$CLICKHOUSE_CLIENT --user $test_user -q "
    system stop $CLICKHOUSE_DATABASE.plain_mt; -- {serverError ACCESS_DENIED}"
$CLICKHOUSE_CLIENT -q "grant system streaming engines on $CLICKHOUSE_DATABASE.plain_mt to $test_user;"
$CLICKHOUSE_CLIENT --user $test_user -q "
    system stop $CLICKHOUSE_DATABASE.plain_mt; -- {serverError BAD_ARGUMENTS}"

$CLICKHOUSE_CLIENT -q "
    select '<9: granted verbs work>';
    select '<9: denied verbs error as expected>';"

$CLICKHOUSE_CLIENT -q "
    system start view granted;
    system stop view granted;
    system stop view denied;
    drop table granted;
    drop table denied;
    drop table plain_mt;
    drop table src;
    drop user $test_user;"

# ---------------------------------------------------------------------------
# Test 10: SYSTEM ... ALL BACKGROUND with partial privileges acts only on the
# objects the caller may control and silently skips the rest, so unauthorized
# background activity keeps running. The wildcard is a per-object fan-out (each
# table is access-checked individually inside startStopAction), not a single
# global lock.
# ---------------------------------------------------------------------------

partial_user="user_04319_partial_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "
    create user $partial_user;
    create table src (x Int64) engine Memory;
    insert into src values (1);
    create materialized view wa refresh every 1 year (x Int64) engine Memory empty as select x from src;
    create materialized view wb refresh every 1 year (x Int64) engine Memory empty as select x from src;
    grant system views on $CLICKHOUSE_DATABASE.wa to $partial_user;"

wait_status wa Scheduled
wait_status wb Scheduled

# The user holds SYSTEM VIEWS on wa only: STOP ALL BACKGROUND must stop wa and leave wb untouched.
$CLICKHOUSE_CLIENT --user $partial_user -q "system stop all background;"

wait_status wa Disabled

$CLICKHOUSE_CLIENT -q "
    select '<10: partial privilege stops only the authorized view>',
        (select status = 'Disabled' from refreshes where view = 'wa'),
        (select status != 'Disabled' from refreshes where view = 'wb');"

$CLICKHOUSE_CLIENT -q "
    system stop view wa;
    system stop view wb;
    drop table wa;
    drop table wb;
    drop table src;
    drop user $partial_user;"
