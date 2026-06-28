#!/usr/bin/env bash
# Tags: atomic-database, memory-engine, no-parallel
# The test uses `SYSTEM PAUSE VIEWS` and `SYSTEM START VIEWS` which affect all
# refreshable views on the server, so it must not run concurrently with other
# tests that create refreshable materialized views.

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

# Helper: wait until the refresh is running AND has read at least one row.
# `wait_status Running` only checks that the task entered the Running state, which is set before
# the pipeline executor starts. In slow CI builds the gap between "state = Running" and the
# executor reading its first row can be long, so a SYSTEM STOP issued right after `wait_status`
# may race with the refresh and find it already finished. Waiting for `read_rows > 0` makes sure
# the executor is actually past setup and inside `executor.execute()`, so the cancellation has a
# concrete pipeline to interrupt.
wait_running_with_progress() {
    local view_name=$1
    while [ "`$CLICKHOUSE_CLIENT -q "select status = 'Running' and read_rows > 0 from refreshes where view = '$view_name' -- $LINENO" | xargs`" != '1' ]
    do
        sleep 0.1
    done
}

# ---------------------------------------------------------------------------
# Test 1: SYSTEM PAUSE VIEW does NOT interrupt the current refresh.
# ---------------------------------------------------------------------------

# A slow refresh: 5 rows * 1 second each, long enough to observe `Running` and pause.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(5) settings max_block_size=1;
    create materialized view p refresh every 1 year (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh view p;"

wait_status p Running

# Pause while the refresh is running. This should NOT cancel the ongoing refresh.
$CLICKHOUSE_CLIENT -q "system pause view p;"

# The currently running refresh must complete successfully.
$CLICKHOUSE_CLIENT -q "system wait view p;"

# After pause, the view must be disabled (no new refresh scheduled).
wait_status p Disabled

$CLICKHOUSE_CLIENT -q "
    select '<1: pause allows current refresh to complete>',
        (select count() from p),
        (select exception = '' from refreshes where view = 'p'),
        (select last_success_time is not null from refreshes where view = 'p');"

# SYSTEM START VIEW should resume a paused view.
$CLICKHOUSE_CLIENT -q "
    truncate src;
    insert into src values (42);
    system start view p;
    system refresh view p;
    system wait view p;
    select '<2: start resumes paused view>', * from p;
    system stop view p;
    drop table p;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 2 (contrast): SYSTEM STOP VIEW DOES interrupt the current refresh.
# ---------------------------------------------------------------------------

# Use 10 source rows (10s of total sleep) to give a comfortable time budget for the SYSTEM STOP
# query to reach the server while the pipeline is still executing - 5 rows turned out to be too
# tight in slow CI builds.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size=1;
    create materialized view s refresh every 1 year (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh view s;"

wait_running_with_progress s

$CLICKHOUSE_CLIENT -q "system stop view s;"

# STOP should interrupt the refresh; status quickly becomes Disabled with a cancellation error.
wait_status s Disabled

$CLICKHOUSE_CLIENT -q "
    select '<3: stop cancels the refresh>',
        (select count() from s),
        (select exception != '' from refreshes where view = 's');
    drop table s;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 3: SYSTEM STOP VIEW after SYSTEM PAUSE VIEW still interrupts the
# in-flight refresh. Regression test for the PAUSE-then-STOP interaction:
# once paused, a subsequent STOP must still cancel the running refresh, even
# though PAUSE has already set the same `stop_requested` guard.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size=1;
    create materialized view ps refresh every 1 year (x Int64) engine Memory empty as
        select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
    system refresh view ps;"

wait_running_with_progress ps

# Pause first; refresh keeps running because PAUSE does not interrupt.
$CLICKHOUSE_CLIENT -q "system pause view ps;"

# Now STOP must still interrupt the in-flight refresh.
$CLICKHOUSE_CLIENT -q "system stop view ps;"

wait_status ps Disabled

$CLICKHOUSE_CLIENT -q "
    select '<3a: stop after pause cancels the refresh>',
        (select count() from ps),
        (select exception != '' from refreshes where view = 'ps');
    drop table ps;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4: SYSTEM PAUSE VIEWS pauses all refreshable views on this replica.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src values (1);
    create materialized view va refresh every 1 second (x Int64) engine Memory empty
        as select x from src;
    create materialized view vb refresh every 1 second (x Int64) engine Memory empty
        as select x from src;
    system wait view va;
    system wait view vb;"

$CLICKHOUSE_CLIENT -q "system pause views;"

wait_status va Disabled
wait_status vb Disabled

$CLICKHOUSE_CLIENT -q "
    select '<4: pause views disables all>',
        countIf(status = 'Disabled'), count()
    from refreshes where view in ('va', 'vb');"

$CLICKHOUSE_CLIENT -q "system start views;"

# After start views, each view should leave the Disabled state.
while [ "`$CLICKHOUSE_CLIENT -q "select countIf(status = 'Disabled') from refreshes where view in ('va', 'vb') -- $LINENO" | xargs`" != '0' ]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    select '<5: start views resumes all>',
        countIf(status != 'Disabled'), count()
    from refreshes where view in ('va', 'vb');
    system stop view va;
    system stop view vb;
    drop table va;
    drop table vb;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4: Access control for SYSTEM PAUSE VIEW.
# ---------------------------------------------------------------------------

test_user="user_04105_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "
    create user $test_user;
    create table src (x Int64) engine Memory;
    insert into src values (1);
    create materialized view granted refresh every 1 year (x Int64) engine Memory empty as select x from src;
    create materialized view denied refresh every 1 year (x Int64) engine Memory empty as select x from src;
    system wait view granted;
    system wait view denied;
    grant system views on $CLICKHOUSE_DATABASE.granted to $test_user;"

# Pausing a view without SYSTEM VIEWS privilege should fail.
$CLICKHOUSE_CLIENT --user $test_user -q "
    system pause view $CLICKHOUSE_DATABASE.denied; -- {serverError ACCESS_DENIED}"

# Pausing a view for which the user has SYSTEM VIEWS should succeed.
$CLICKHOUSE_CLIENT --user $test_user -q "
    system pause view $CLICKHOUSE_DATABASE.granted;"
wait_status granted Disabled

echo '<6: granted pause works>'
echo '<6: denied pause errors as expected>'

$CLICKHOUSE_CLIENT -q "
    system stop view granted;
    system stop view denied;
    drop table granted;
    drop table denied;
    drop table src;
    drop user $test_user;"
