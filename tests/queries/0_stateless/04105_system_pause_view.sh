#!/usr/bin/env bash
# Tags: atomic-database, memory-engine

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

# Helper: create refreshable view `$1` over `src` (10 rows, 1 second each), refresh it, and send the
# statements `$2` while the refresh is still running. The check and `$2` go in one client call, so no
# client start-up separates them; if a slow server let the refresh get too far before the check, the
# view is re-created and the attempt repeated. Prints 1 if `$2` was sent while the refresh was running.
interrupt_running_refresh() {
    local view_name=$1
    local statements=$2
    local running=0
    for _ in {1..10}
    do
        $CLICKHOUSE_CLIENT -q "
            drop table if exists $view_name;
            create materialized view $view_name refresh every 1 year (x Int64) engine Memory empty as
                select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
            system refresh view $view_name;"
        # At most 8 of the 10 rows read: at least 2 seconds of the refresh are left.
        running=`$CLICKHOUSE_CLIENT -q "
            select status = 'Running' and read_rows between 1 and 8 from refreshes where view = '$view_name';
            $statements"`
        [ "$running" = 1 ] && break
    done
    echo "$running"
}

# ---------------------------------------------------------------------------
# Test 1: SYSTEM PAUSE VIEW does NOT interrupt the current refresh.
# ---------------------------------------------------------------------------

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(2) settings max_block_size=1;"

# Pause while a slow refresh (2 rows * 1 second) is running; PAUSE must not cancel it. On a slow server
# the pause can land before the refresh starts or after it ends, so retry with a new view until the
# refresh is still running right after the pause.
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT -q "
        drop table if exists p;
        create materialized view p refresh every 1 year (x Int64) engine Memory empty as
            select x + sleepEachRow(1) as x from src settings max_block_size = 1, max_threads = 1;
        system refresh view p;"
    # 1 = the refresh started before the pause (`Running`) and had not swapped in its result yet (`p` empty).
    running=`$CLICKHOUSE_CLIENT -q "
        system pause view p;
        select status = 'Running' and (select count() from p) = 0 from refreshes where view = 'p' -- $LINENO"`
    [ "$running" = 1 ] && break
done
echo "<1: refresh running after pause: $running>"

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

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(10) settings max_block_size=1;"

running=`interrupt_running_refresh s "system stop view s;"`
echo "<3: refresh running before stop: $running>"

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
    insert into src select * from numbers(10) settings max_block_size=1;"

# Pause first; the refresh keeps running because PAUSE does not interrupt. Then STOP must still
# interrupt the in-flight refresh.
running=`interrupt_running_refresh ps "system pause view ps; system stop view ps;"`
echo "<3a: refresh running before stop: $running>"

wait_status ps Disabled

$CLICKHOUSE_CLIENT -q "
    select '<3a: stop after pause cancels the refresh>',
        (select count() from ps),
        (select exception != '' from refreshes where view = 'ps');
    drop table ps;
    drop table src;"

# ---------------------------------------------------------------------------
# Test 4: SYSTEM PAUSE VIEWS pauses all refreshable views on this replica.
# It skips views the caller may not control, so it runs as a user granted only on this database,
# to leave the views of concurrent tests alone.
# ---------------------------------------------------------------------------

db_user="user_04105_db_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "
    create user $db_user;
    grant system views on $CLICKHOUSE_DATABASE.* to $db_user;"

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src values (1);
    create materialized view va refresh every 1 second (x Int64) engine Memory empty
        as select x from src;
    create materialized view vb refresh every 1 second (x Int64) engine Memory empty
        as select x from src;
    system wait view va;
    system wait view vb;"

$CLICKHOUSE_CLIENT --user $db_user -q "system pause views;"

wait_status va Disabled
wait_status vb Disabled

$CLICKHOUSE_CLIENT -q "
    select '<4: pause views disables all>',
        countIf(status = 'Disabled'), count()
    from refreshes where view in ('va', 'vb');"

$CLICKHOUSE_CLIENT --user $db_user -q "system start views;"

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
    drop table src;
    drop user $db_user;"

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
