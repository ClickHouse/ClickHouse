#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Set session timezone to UTC to make all DateTime formatting and parsing use UTC, because refresh
# scheduling is done in UTC.
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --allow_experimental_refreshable_materialized_view=1 --session_timezone Etc/UTC"`"

$CLICKHOUSE_CLIENT -nq "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"


# Basic refreshing.
$CLICKHOUSE_CLIENT -nq "
    create materialized view a
        refresh after 2 second
        engine Memory
        empty
        as select number as x from numbers(2) union all select rand64() as x"
$CLICKHOUSE_CLIENT -nq "select '<1: created view>', view, remaining_dependencies, exception, last_refresh_result in ('Unknown', 'Finished') from refreshes";
$CLICKHOUSE_CLIENT -nq "show create a"
# Wait for any refresh. (xargs trims the string and turns \t and \n into spaces)
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_result from refreshes -- $LINENO" | xargs`" == 'Unknown' ]
do
    sleep 0.1
done
start_time="`$CLICKHOUSE_CLIENT -nq "select reinterpret(now64(), 'Int64')"`"
# Check table contents.
$CLICKHOUSE_CLIENT -nq "select '<2: refreshed>', count(), sum(x=0), sum(x=1) from a"
# Wait for table contents to change.
res1="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values'`"
while :
do
    res2="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values -- $LINENO'`"
    [ "$res2" == "$res1" ] || break
    sleep 0.1
done
# Wait for another change.
while :
do
    res3="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values -- $LINENO'`"
    [ "$res3" == "$res2" ] || break
    sleep 0.1
done
# Check that the two changes were at least 1 second apart, in particular that we're not refreshing
# like crazy. This is potentially flaky, but we need at least one test that uses non-mocked timer
# to make sure the clock+timer code works at all. If it turns out flaky, increase refresh period above.
$CLICKHOUSE_CLIENT -nq "
    select '<3: time difference at least>', min2(reinterpret(now64(), 'Int64') - $start_time, 1000);
    select '<4: next refresh in>', next_refresh_time-last_refresh_time from refreshes;"

# Create a source table from which views will read.
$CLICKHOUSE_CLIENT -nq "
    create table src (x Int8) engine Memory as select 1"

# Switch to fake clock, change refresh schedule, change query.
$CLICKHOUSE_CLIENT -nq "
    system test view a set fake time '2050-01-01 00:00:01';"
while [ "`$CLICKHOUSE_CLIENT -nq "select status, last_refresh_time, next_refresh_time from refreshes -- $LINENO" | xargs`" != 'Scheduled 2050-01-01 00:00:01 2050-01-01 00:00:03' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    alter table a modify refresh every 2 year;
    alter table a modify query select x*2 as x from src;
    select '<4.5: altered>', status, last_refresh_result, next_refresh_time from refreshes;
    show create a;"
# Advance time to trigger the refresh.
$CLICKHOUSE_CLIENT -nq "
    select '<5: no refresh>', count() from a;
    system test view a set fake time '2052-02-03 04:05:06';"
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_time from refreshes -- $LINENO" | xargs`" != '2052-02-03 04:05:06' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<6: refreshed>', * from a;
    select '<7: refreshed>', status, last_refresh_result, next_refresh_time from refreshes;"

# Create a dependent view, refresh it once.
$CLICKHOUSE_CLIENT -nq "
    create materialized view b refresh every 2 year depends on a (y Int32) engine MergeTree order by y empty as select x*10 as y from a;
    show create b;
    system test view b set fake time '2052-11-11 11:11:11';
    system refresh view b;"
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != '2052-11-11 11:11:11' ]
do
    sleep 0.1
done
# Next refresh shouldn't start until the dependency refreshes.
$CLICKHOUSE_CLIENT -nq "
    select '<8: refreshed>', * from b;
    select '<9: refreshed>', view, status, last_refresh_result, next_refresh_time from refreshes;
    system test view b set fake time '2054-01-24 23:22:21';"
while [ "`$CLICKHOUSE_CLIENT -nq "select status, next_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != 'WaitingForDependencies 2054-01-01 00:00:00' ]
do
    sleep 0.1
done
# Update source table (by dropping and re-creating it - to test that tables are looked up by name
# rather than uuid), kick off refresh of the dependency.
$CLICKHOUSE_CLIENT -nq "
    select '<10: waiting>', view, status, remaining_dependencies, next_refresh_time from refreshes;
    drop table src;
    create table src (x Int16) engine Memory as select 2;
    system test view a set fake time '2054-01-01 00:00:01';"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes where view = 'b' -- $LINENO" | xargs`" != 'Scheduled' ]
do
    sleep 0.1
done
# Both tables should've refreshed.
$CLICKHOUSE_CLIENT -nq "
    select '<11: chain-refreshed a>', * from a;
    select '<12: chain-refreshed b>', * from b;
    select '<13: chain-refreshed>', view, status, remaining_dependencies, last_refresh_result, last_refresh_time, next_refresh_time, exception from refreshes;"

# Make the dependent table run ahead by one refresh cycle, make sure it waits for the dependency to
# catch up to the same cycle.
$CLICKHOUSE_CLIENT -nq "
    system test view b set fake time '2059-01-01 00:00:00';
    system refresh view b;"
while [ "`$CLICKHOUSE_CLIENT -nq "select next_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != '2060-01-01 00:00:00' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    system test view b set fake time '2061-01-01 00:00:00';
    system test view a set fake time '2057-01-01 00:00:00';"
while [ "`$CLICKHOUSE_CLIENT -nq "select status, next_refresh_time from refreshes -- $LINENO" | xargs`" != 'Scheduled 2058-01-01 00:00:00 WaitingForDependencies 2060-01-01 00:00:00' ]
do
    sleep 0.1
done
sleep 1
$CLICKHOUSE_CLIENT -nq "
    select '<14: waiting for next cycle>', view, status, remaining_dependencies, next_refresh_time from refreshes;
    truncate src;
    insert into src values (3);
    system test view a set fake time '2060-02-02 02:02:02';"
while [ "`$CLICKHOUSE_CLIENT -nq "select next_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != '2062-01-01 00:00:00' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<15: chain-refreshed a>', * from a;
    select '<16: chain-refreshed b>', * from b;
    select '<17: chain-refreshed>', view, status, next_refresh_time from refreshes;"

# Get to WaitingForDependencies state and remove the depencency.
$CLICKHOUSE_CLIENT -nq "
    system test view b set fake time '2062-03-03 03:03:03'"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes where view = 'b' -- $LINENO" | xargs`" != 'WaitingForDependencies' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    alter table b modify refresh every 2 year"
while [ "`$CLICKHOUSE_CLIENT -nq "select status, last_refresh_time from refreshes where view = 'b' -- $LINENO" | xargs`" != 'Scheduled 2062-03-03 03:03:03' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<18: removed dependency>', view, status, remaining_dependencies, last_refresh_time,next_refresh_time, refresh_count from refreshes where view = 'b';
    show create b;"

# Select from a table that doesn't exist, get an exception.
$CLICKHOUSE_CLIENT -nq "
    drop table a;
    drop table b;
    create materialized view c refresh every 1 second (x Int64) engine Memory empty as select * from src;
    drop table src;"
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_result from refreshes -- $LINENO" | xargs`" != 'Exception' ]
do
    sleep 0.1
done
# Check exception, create src, expect successful refresh.
$CLICKHOUSE_CLIENT -nq "
    select '<19: exception>', exception ilike '%UNKNOWN_TABLE%' from refreshes;
    create table src (x Int64) engine Memory as select 1;
    system refresh view c;"
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_result from refreshes -- $LINENO" | xargs`" != 'Finished' ]
do
    sleep 0.1
done
# Rename table.
$CLICKHOUSE_CLIENT -nq "
    select '<20: unexception>', * from c;
    rename table c to d;
    select '<21: rename>', * from d;
    select '<22: rename>', view, last_refresh_result from refreshes;"

# Do various things during a refresh.
# First make a nonempty view.
$CLICKHOUSE_CLIENT -nq "
    drop table d;
    truncate src;
    insert into src values (1)
    create materialized view e refresh every 1 second (x Int64) engine MergeTree order by x empty as select x + sleepEachRow(1) as x from src settings max_block_size = 1;"
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_result from refreshes -- $LINENO" | xargs`" != 'Finished' ]
do
    sleep 0.1
done
# Stop refreshes.
$CLICKHOUSE_CLIENT -nq "
    select '<23: simple refresh>', * from e;
    system stop view e;"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes -- $LINENO" | xargs`" != 'Disabled' ]
do
    sleep 0.1
done
# Make refreshes slow, wait for a slow refresh to start. (We stopped refreshes first to make sure
# we wait for a slow refresh, not a previous fast one.)
$CLICKHOUSE_CLIENT -nq "
    insert into src select * from numbers(1000) settings max_block_size=1;
    system start view e;"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes -- $LINENO" | xargs`" != 'Running' ]
do
    sleep 0.1
done
# Rename.
$CLICKHOUSE_CLIENT -nq "
    rename table e to f;
    select '<24: rename during refresh>', * from f;
    select '<25: rename during refresh>', view, status from refreshes;
    alter table f modify refresh after 10 year;"
sleep 2 # make it likely that at least one row was processed
# Cancel.
$CLICKHOUSE_CLIENT -nq "
    system cancel view f;"
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_result from refreshes -- $LINENO" | xargs`" != 'Cancelled' ]
do
    sleep 0.1
done
# Check that another refresh doesn't immediately start after the cancelled one.
sleep 1
$CLICKHOUSE_CLIENT -nq "
    select '<27: cancelled>', view, status from refreshes;
    system refresh view f;"
while [ "`$CLICKHOUSE_CLIENT -nq "select status from refreshes -- $LINENO" | xargs`" != 'Running' ]
do
    sleep 0.1
done
# Drop.
$CLICKHOUSE_CLIENT -nq "
    drop table f;
    select '<28: drop during refresh>', view, status from refreshes;"

# Try OFFSET and RANDOMIZE FOR.
$CLICKHOUSE_CLIENT -nq "
    create materialized view g refresh every 1 week offset 3 day 4 hour randomize for 4 day 1 hour (x Int64) engine Memory empty as select 42;
    show create g;
    system test view g set fake time '2050-02-03 15:30:13';"
while [ "`$CLICKHOUSE_CLIENT -nq "select next_refresh_time > '2049-01-01' from refreshes -- $LINENO" | xargs`" != '1' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    with '2050-02-10 04:00:00'::DateTime as expected
    select '<29: randomize>', abs(next_refresh_time::Int64 - expected::Int64) <= 3600*(24*4+1), next_refresh_time != expected from refreshes;"

# Send data 'TO' an existing table.
$CLICKHOUSE_CLIENT -nq "
    drop table g;
    create table dest (x Int64) engine MergeTree order by x;
    truncate src;
    insert into src values (1);
    create materialized view h refresh every 1 second to dest empty as select x*10 as x from src;
    show create h;"
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_result from refreshes -- $LINENO" | xargs`" != 'Finished' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<30: to existing table>', * from dest;
    insert into src values (2);"
while [ "`$CLICKHOUSE_CLIENT -nq "select count() from dest -- $LINENO" | xargs`" != '2' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<31: to existing table>', * from dest;
    drop table dest;
    drop table src;
    drop table h;"

# EMPTY
$CLICKHOUSE_CLIENT -nq "
    create materialized view i refresh after 1 year engine Memory empty as select number as x from numbers(2);
    create materialized view j refresh after 1 year engine Memory as select number as x from numbers(2)"
while [ "`$CLICKHOUSE_CLIENT -nq "select sum(last_success_time is null) from refreshes -- $LINENO" | xargs`" == '2' ]
do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -nq "
    select '<32: empty>', view, status, last_refresh_result from refreshes order by view;
    drop table i;
    drop table j"

$CLICKHOUSE_CLIENT -nq "
    drop table refreshes;"
