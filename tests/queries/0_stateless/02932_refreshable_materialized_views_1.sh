#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Set session timezone to UTC to make all DateTime formatting and parsing use UTC, because refresh
# scheduling is done in UTC.
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --session_timezone Etc/UTC"`"

# system.view_refreshes briefly reports the transient internal state 'Scheduling' between
# state transitions of the refresh task. Use this helper for SELECTs that read the status
# column: it retries until no row reports 'Scheduling'.
query_no_scheduling() {
    local out
    while :
    do
        out=$($CLICKHOUSE_CLIENT -q "$1")
        if ! grep -qE $'(^|\t)Scheduling(\t|$)' <<< "$out"; then
            echo "$out"
            return
        fi
        sleep 0.2
    done
}

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"


# Basic refreshing.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_a
        refresh every 2 second
        engine Memory
        empty
        as select number as x from numbers(2) union all select rand64() as x;
    select '<1: created view>', exception, view from refreshes;
    show create rmv_a;"
# Wait for any refresh. (xargs trims the string and turns \t and \n into spaces)
while [ "`$CLICKHOUSE_CLIENT -q "select last_success_time is null from refreshes -- $LINENO" | xargs`" != '0' ]
do
    sleep 0.5
done
start_time="`$CLICKHOUSE_CLIENT -q "select reinterpret(now64(), 'Int64')"`"
# Check table contents.
$CLICKHOUSE_CLIENT -q "select '<2: refreshed>', count(), sum(x=0), sum(x=1) from rmv_a"
# Wait for table contents to change.
res1="`$CLICKHOUSE_CLIENT -q 'select * from rmv_a order by x format Values'`"
while :
do
    res2="`$CLICKHOUSE_CLIENT -q 'select * from rmv_a order by x format Values -- $LINENO'`"
    [ "$res2" == "$res1" ] || break
    sleep 0.5
done
# Wait for another change.
while :
do
    res3="`$CLICKHOUSE_CLIENT -q 'select * from rmv_a order by x format Values -- $LINENO'`"
    [ "$res3" == "$res2" ] || break
    sleep 0.5
done
# Check that the two changes were at least 1 second apart, in particular that we're not refreshing
# like crazy. This is potentially flaky, but we need at least one test that uses non-mocked timer
# to make sure the clock+timer code works at all. If it turns out flaky, increase refresh period above.
$CLICKHOUSE_CLIENT -q "
    select '<3: time difference at least>', min2(reinterpret(now64(), 'Int64') - $start_time, 1000);"

# Create a source table from which views will read.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int8) engine Memory as select 1;"

# Switch to fake clock, change refresh schedule, change query.
$CLICKHOUSE_CLIENT -q "
    system test view rmv_a set fake time '2050-01-01 00:00:01';
    system wait view rmv_a;
    system refresh view rmv_a;
    system wait view rmv_a;"
query_no_scheduling "select '<4.1: fake clock>', status, last_success_time, next_refresh_time, progress, read_rows, total_rows, written_rows, retry from refreshes"
$CLICKHOUSE_CLIENT -q "
    alter table rmv_a modify refresh every 2 year;
    alter table rmv_a modify query select x*2 as x from src;
    system wait view rmv_a;"
query_no_scheduling "select '<4.5: altered>', status, last_success_time, next_refresh_time from refreshes"
$CLICKHOUSE_CLIENT -q "show create rmv_a;"
# Advance time to trigger the refresh.
$CLICKHOUSE_CLIENT -q "
    select '<5: no refresh>', count() from rmv_a;
    system test view rmv_a set fake time '2052-02-03 04:05:06';"
while [ "`$CLICKHOUSE_CLIENT -q "select last_success_time, status from refreshes -- $LINENO" | xargs`" != '2052-02-03 04:05:06 Scheduled' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "select '<6: refreshed>', * from rmv_a;"
query_no_scheduling "select '<7: refreshed>', status, last_success_time, next_refresh_time from refreshes"

# Create a dependent view, refresh it once.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_b refresh every 2 year depends on rmv_a (y Int32) engine MergeTree order by y empty as select x*10 as y from rmv_a;
    show create rmv_b;
    system test view rmv_b set fake time '2052-11-11 11:11:11';
    system refresh view rmv_b;
    system wait view rmv_b;
    select '<7.5: created dependent>', last_success_time from refreshes where view = 'rmv_b';"
# Next refresh shouldn't start until the dependency refreshes.
$CLICKHOUSE_CLIENT -q "select '<8: refreshed>', * from rmv_b;"
query_no_scheduling "select '<9: refreshed>', view, status, next_refresh_time from refreshes"
$CLICKHOUSE_CLIENT -q "system test view rmv_b set fake time '2054-01-24 23:22:21';"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'rmv_b' -- $LINENO" | xargs`" != 'WaitingForDependencies' ]
do
    sleep 0.5
done

# Drop the source table, check that refresh fails and doesn't leave a temp table behind.
$CLICKHOUSE_CLIENT -q "
    select '<9.2: dropping>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase();
    drop table src;
    system refresh view rmv_a;"
$CLICKHOUSE_CLIENT -q "system wait view rmv_a; -- { serverError REFRESH_FAILED }"
$CLICKHOUSE_CLIENT -q "
    select '<9.4: dropped>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase();"

# Create the source table again, check that refresh succeeds (in particular that tables are looked
# up by name rather than uuid).
query_no_scheduling "select '<10: creating>', view, status, next_refresh_time from refreshes"
$CLICKHOUSE_CLIENT -q "
    create table src (x Int16) engine Memory as select 2;
    system test view rmv_a set fake time '2054-01-01 00:00:01';"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'rmv_b' -- $LINENO" | xargs`" != 'Scheduled' ]
do
    sleep 0.5
done
# Both tables should've refreshed.
$CLICKHOUSE_CLIENT -q "
    select '<11: chain-refreshed rmv_a>', * from rmv_a;
    select '<12: chain-refreshed rmv_b>', * from rmv_b;"
query_no_scheduling "select '<13: chain-refreshed>', view, status, last_success_time, last_refresh_time, next_refresh_time, exception == '' from refreshes"

$CLICKHOUSE_CLIENT -q "
    system test view rmv_b set fake time '2061-01-01 00:00:00';
    truncate src;
    insert into src values (3);
    system test view rmv_a set fake time '2060-02-02 02:02:02';"
while [ "`$CLICKHOUSE_CLIENT -q "select next_refresh_time from refreshes where view = 'rmv_b' -- $LINENO" | xargs`" != '2062-01-01 00:00:00' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<15: chain-refreshed rmv_a>', * from rmv_a;
    select '<16: chain-refreshed rmv_b>', * from rmv_b;"
query_no_scheduling "select '<17: chain-refreshed>', view, status, next_refresh_time from refreshes"

# Get to WaitingForDependencies state and remove the depencency.
$CLICKHOUSE_CLIENT -q "
    system test view rmv_b set fake time '2062-03-03 03:03:03'"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'rmv_b' -- $LINENO" | xargs`" != 'WaitingForDependencies' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    alter table rmv_b modify refresh every 2 year"
while [ "`$CLICKHOUSE_CLIENT -q "select status, last_refresh_time from refreshes where view = 'rmv_b' -- $LINENO" | xargs`" != 'Scheduled 2062-03-03 03:03:03' ]
do
    sleep 0.5
done
query_no_scheduling "select '<18: removed dependency>', view, status, last_success_time, last_refresh_time, next_refresh_time from refreshes where view = 'rmv_b'"
$CLICKHOUSE_CLIENT -q "show create rmv_b;"

# Can't use the same time unit multiple times.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_c refresh every 1 second 2 second (x Int64) engine Memory empty as select * from src; -- { clientError SYNTAX_ERROR }"

$CLICKHOUSE_CLIENT -q "
    drop table src;
    drop table rmv_a;
    drop table rmv_b;
    drop table refreshes;"
