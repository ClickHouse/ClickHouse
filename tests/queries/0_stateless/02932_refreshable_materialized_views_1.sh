#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Set session timezone to UTC to make all DateTime formatting and parsing use UTC, because refresh
# scheduling is done in UTC.
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --allow_experimental_refreshable_materialized_view=1 --session_timezone Etc/UTC"`"

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"


# Basic refreshing.
$CLICKHOUSE_CLIENT -q "
    create materialized view a
        refresh after 2 second
        engine Memory
        empty
        as select number as x from numbers(2) union all select rand64() as x;
    select '<1: created view>', view, remaining_dependencies, exception, last_refresh_result in ('Unknown', 'Finished') from refreshes;
    show create a;"
# Wait for any refresh. (xargs trims the string and turns \t and \n into spaces)
while [ "`$CLICKHOUSE_CLIENT -q "select last_refresh_result from refreshes -- $LINENO" | xargs`" == 'Unknown' ]
do
    sleep 0.5
done
start_time="`$CLICKHOUSE_CLIENT -q "select reinterpret(now64(), 'Int64')"`"
# Check table contents.
$CLICKHOUSE_CLIENT -q "select '<2: refreshed>', count(), sum(x=0), sum(x=1) from a"
# Wait for table contents to change.
res1="`$CLICKHOUSE_CLIENT -q 'select * from a order by x format Values'`"
while :
do
    res2="`$CLICKHOUSE_CLIENT -q 'select * from a order by x format Values -- $LINENO'`"
    [ "$res2" == "$res1" ] || break
    sleep 0.5
done
# Wait for another change.
while :
do
    res3="`$CLICKHOUSE_CLIENT -q 'select * from a order by x format Values -- $LINENO'`"
    [ "$res3" == "$res2" ] || break
    sleep 0.5
done
# Check that the two changes were at least 1 second apart, in particular that we're not refreshing
# like crazy. This is potentially flaky, but we need at least one test that uses non-mocked timer
# to make sure the clock+timer code works at all. If it turns out flaky, increase refresh period above.
$CLICKHOUSE_CLIENT -q "
    select '<3: time difference at least>', min2(reinterpret(now64(), 'Int64') - $start_time, 1000);
    select '<4: next refresh in>', next_refresh_time-last_refresh_time from refreshes;"

# Create a source table from which views will read.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int8) engine Memory as select 1;"

# Switch to fake clock, change refresh schedule, change query.
$CLICKHOUSE_CLIENT -q "
    system test view a set fake time '2050-01-01 00:00:01';
    system wait view a;
    system refresh view a;
    system wait view a;
    select '<4.1: fake clock>', status, last_refresh_time, next_refresh_time from refreshes;
    alter table a modify refresh every 2 year;
    alter table a modify query select x*2 as x from src;
    select '<4.5: altered>', status, last_refresh_result, next_refresh_time from refreshes;
    show create a;"
# Advance time to trigger the refresh.
$CLICKHOUSE_CLIENT -q "
    select '<5: no refresh>', count() from a;
    system test view a set fake time '2052-02-03 04:05:06';"
while [ "`$CLICKHOUSE_CLIENT -q "select last_refresh_time from refreshes -- $LINENO" | xargs`" != '2052-02-03 04:05:06' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<6: refreshed>', * from a;
    select '<7: refreshed>', status, last_refresh_result, next_refresh_time from refreshes;"

# Create a dependent view, refresh it once.
$CLICKHOUSE_CLIENT -q "
    create materialized view b refresh every 2 year depends on a (y Int32) engine MergeTree order by y empty as select x*10 as y from a;
    show create b;
    system test view b set fake time '2052-11-11 11:11:11';
    system refresh view b;
    system wait view b;
    select '<7.5: created dependent>', last_refresh_time from refreshes where view = 'b';"
# Next refresh shouldn't start until the dependency refreshes.
$CLICKHOUSE_CLIENT -q "
    select '<8: refreshed>', * from b;
    select '<9: refreshed>', view, status, last_refresh_result, next_refresh_time from refreshes;
    system test view b set fake time '2054-01-24 23:22:21';"
while [ "`$CLICKHOUSE_CLIENT -q "select status, next_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != 'WaitingForDependencies 2054-01-01 00:00:00' ]
do
    sleep 0.5
done

# Drop the source table, check that refresh fails and doesn't leave a temp table behind.
$CLICKHOUSE_CLIENT -q "
    select '<9.2: dropping>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase();
    drop table src;
    system refresh view a;"
$CLICKHOUSE_CLIENT -q "system wait view a;" 2>/dev/null && echo "SYSTEM WAIT VIEW failed to fail at $LINENO"
$CLICKHOUSE_CLIENT -q "
    select '<9.4: dropped>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase();"

# Create the source table again, check that refresh succeeds (in particular that tables are looked
# up by name rather than uuid).
$CLICKHOUSE_CLIENT -q "
    select '<10: creating>', view, status, remaining_dependencies, next_refresh_time from refreshes;
    create table src (x Int16) engine Memory as select 2;
    system test view a set fake time '2054-01-01 00:00:01';"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'b' -- $LINENO" | xargs`" != 'Scheduled' ]
do
    sleep 0.5
done
# Both tables should've refreshed.
$CLICKHOUSE_CLIENT -q "
    select '<11: chain-refreshed a>', * from a;
    select '<12: chain-refreshed b>', * from b;
    select '<13: chain-refreshed>', view, status, remaining_dependencies, last_refresh_result, last_refresh_time, next_refresh_time, exception == '' from refreshes;"

# Make the dependent table run ahead by one refresh cycle, make sure it waits for the dependency to
# catch up to the same cycle.
$CLICKHOUSE_CLIENT -q "
    system test view b set fake time '2059-01-01 00:00:00';
    system refresh view b;"
while [ "`$CLICKHOUSE_CLIENT -q "select next_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != '2060-01-01 00:00:00' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    system test view b set fake time '2061-01-01 00:00:00';
    system test view a set fake time '2057-01-01 00:00:00';"
while [ "`$CLICKHOUSE_CLIENT -q "select status, next_refresh_time from refreshes -- $LINENO" | xargs`" != 'Scheduled 2058-01-01 00:00:00 WaitingForDependencies 2060-01-01 00:00:00' ]
do
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "
    select '<14: waiting for next cycle>', view, status, remaining_dependencies, next_refresh_time from refreshes;
    truncate src;
    insert into src values (3);
    system test view a set fake time '2060-02-02 02:02:02';"
while [ "`$CLICKHOUSE_CLIENT -q "select next_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != '2062-01-01 00:00:00' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<15: chain-refreshed a>', * from a;
    select '<16: chain-refreshed b>', * from b;
    select '<17: chain-refreshed>', view, status, next_refresh_time from refreshes;"

# Get to WaitingForDependencies state and remove the depencency.
$CLICKHOUSE_CLIENT -q "
    system test view b set fake time '2062-03-03 03:03:03'"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'b' -- $LINENO" | xargs`" != 'WaitingForDependencies' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    alter table b modify refresh every 2 year"
while [ "`$CLICKHOUSE_CLIENT -q "select status, last_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != 'Scheduled 2062-03-03 03:03:03' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<18: removed dependency>', view, status, remaining_dependencies, last_refresh_time,next_refresh_time, refresh_count from refreshes where view = 'b';
    show create b;"

$CLICKHOUSE_CLIENT -q "
    drop table src;
    drop table a;
    drop table b;
    drop table refreshes;"
