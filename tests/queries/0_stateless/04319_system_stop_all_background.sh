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
# Test 4b: SYSTEM REFRESH on a STOPPED view is deferred, not dropped. While the
# view is stopped the request is remembered but does not run (the view stays
# Disabled and empty); it fires when SYSTEM START releases the view. This is the
# same behavior as SYSTEM REFRESH VIEW on a stopped view, and it differs from the
# streaming engines, where REFRESH runs exactly one cycle even while stopped.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src values (44);
    create materialized view d refresh every 1 year (x Int64) engine Memory empty as
        select x from src;
    system stop d;"

wait_status d Disabled

# REFRESH while stopped: the request is remembered, the view stays Disabled and empty for now.
$CLICKHOUSE_CLIENT -q "
    system refresh d;
    select '<4b: refresh while stopped is deferred>',
        (select count() from d),
        (select status from refreshes where view = 'd');"

# START releases the view and the deferred refresh fires, producing the row.
$CLICKHOUSE_CLIENT -q "system start d;"
while [ "`$CLICKHOUSE_CLIENT -q "select count() from d -- $LINENO" | xargs`" == '0' ]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    select '<4b: deferred refresh fires on start>', * from d;
    system stop view d;
    drop table d;
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

echo '<9: granted verbs work>'
echo '<9: denied verbs error as expected>'

$CLICKHOUSE_CLIENT -q "
    system start view granted;
    system stop view granted;
    system stop view denied;
    drop table granted;
    drop table denied;
    drop table src;
    drop user $test_user;"
