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
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --allow_experimental_refreshable_materialized_view=1 --allow_materialized_view_with_bad_select=0 --session_timezone Etc/UTC"`"

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"


# Select from a table that doesn't exist, get an exception.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int8) engine Memory as select 1;
    create materialized view c refresh every 1 second (x Int64) engine Memory empty as select * from src;
    drop table src;"
while [ "`$CLICKHOUSE_CLIENT -q "select last_refresh_result from refreshes where view = 'c' -- $LINENO" | xargs`" != 'Error' ]
do
    sleep 0.5
done
# Check exception, create src, expect successful refresh.
$CLICKHOUSE_CLIENT -q "
    select '<19: exception>', exception ilike '%UNKNOWN_TABLE%' ? '1' : exception from refreshes where view = 'c';
    create table src (x Int64) engine Memory as select 1;
    system refresh view c;"
while [ "`$CLICKHOUSE_CLIENT -q "select last_refresh_result from refreshes -- $LINENO" | xargs`" != 'Finished' ]
do
    sleep 0.5
done
# Rename table.
$CLICKHOUSE_CLIENT -q "
    select '<20: unexception>', * from c;
    rename table c to d;
    select '<21: rename>', * from d;
    select '<22: rename>', view, last_refresh_result from refreshes;"

# Do various things during a refresh.
# First make a nonempty view.
$CLICKHOUSE_CLIENT -q "
    drop table d;
    truncate src;
    insert into src values (1);
    create materialized view e refresh every 1 second (x Int64) engine MergeTree order by x empty as select x + sleepEachRow(1) as x from src settings max_block_size = 1;"
while [ "`$CLICKHOUSE_CLIENT -q "select last_refresh_result from refreshes -- $LINENO" | xargs`" != 'Finished' ]
do
    sleep 0.5
done
# Stop refreshes.
$CLICKHOUSE_CLIENT -q "
    select '<23: simple refresh>', * from e;
    system stop view e;"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes -- $LINENO" | xargs`" != 'Disabled' ]
do
    sleep 0.5
done
# Make refreshes slow, wait for a slow refresh to start. (We stopped refreshes first to make sure
# we wait for a slow refresh, not a previous fast one.)
$CLICKHOUSE_CLIENT -q "
    insert into src select * from numbers(1000) settings max_block_size=1;
    system start view e;"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes -- $LINENO" | xargs`" != 'Running' ]
do
    sleep 0.5
done
# Rename.
$CLICKHOUSE_CLIENT -q "
    rename table e to f;
    select '<24: rename during refresh>', * from f;
    select '<25: rename during refresh>', view, status from refreshes where view = 'f';
    alter table f modify refresh after 10 year;"

# Cancel.
$CLICKHOUSE_CLIENT -q "
    system cancel view f;"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'f' -- $LINENO" | xargs`" != 'Scheduled' ]
do
    sleep 0.5
done
# Check that another refresh doesn't immediately start after the cancelled one.
$CLICKHOUSE_CLIENT -q "
    select '<27: cancelled>', view, status, last_refresh_result from refreshes where view = 'f';
    system refresh view f;"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'f' -- $LINENO" | xargs`" != 'Running' ]
do
    sleep 0.5
done
# Drop.
$CLICKHOUSE_CLIENT -q "
    drop table f;
    select '<28: drop during refresh>', view, status from refreshes;
    select '<28: drop during refresh>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase()"

# Try OFFSET and RANDOMIZE FOR.
$CLICKHOUSE_CLIENT -q "
    create materialized view g refresh every 1 week offset 3 day 4 hour randomize for 4 day 1 hour (x Int64) engine Memory empty as select 42 as x;
    show create g;
    system test view g set fake time '2050-02-03 15:30:13';"
while [ "`$CLICKHOUSE_CLIENT -q "select next_refresh_time > '2049-01-01' from refreshes -- $LINENO" | xargs`" != '1' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    with '2050-02-10 04:00:00'::DateTime as expected
    select '<29: randomize>', abs(next_refresh_time::Int64 - expected::Int64) <= 3600*(24*4+1), next_refresh_time != expected from refreshes;"

# Send data 'TO' an existing table.
$CLICKHOUSE_CLIENT -q "
    drop table g;
    create table dest (x Int64) engine MergeTree order by x;
    truncate src;
    insert into src values (1);
    create materialized view h refresh every 1 second to dest empty as select x*10 as x from src;
    show create h;"
while [ "`$CLICKHOUSE_CLIENT -q "select last_refresh_result from refreshes -- $LINENO" | xargs`" != 'Finished' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<30: to existing table>', * from dest;
    insert into src values (2);"
while [ "`$CLICKHOUSE_CLIENT -q "select count() from dest -- $LINENO" | xargs`" != '2' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<31: to existing table>', * from dest;
    drop table dest;
    drop table h;"

# Retries.
$CLICKHOUSE_CLIENT -q "
    create materialized view h2 refresh after 1 year settings refresh_retries = 10 (x Int64) engine Memory as select x*10 + throwIf(x % 2 == 0) as x from src;"
$CLICKHOUSE_CLIENT -q "system wait view h2;" 2>/dev/null && echo "SYSTEM WAIT VIEW failed to fail at $LINENO"
$CLICKHOUSE_CLIENT -q "
    select '<31.5: will retry>', last_refresh_result, retry > 0 from refreshes;
    create table src2 (x Int8) engine Memory;
    insert into src2 values (1);
    exchange tables src and src2;
    drop table src2;"
while [ "`$CLICKHOUSE_CLIENT -q "select last_refresh_result, retry from refreshes -- $LINENO" | xargs`" != 'Finished 0' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<31.6: did retry>', x from h2;
    drop table h2"

# EMPTY
$CLICKHOUSE_CLIENT -q "
    create materialized view i refresh after 1 year engine Memory empty as select number as x from numbers(2);
    create materialized view j refresh after 1 year engine Memory as select number as x from numbers(2);"
while [ "`$CLICKHOUSE_CLIENT -q "select sum(last_success_time is null) from refreshes -- $LINENO" | xargs`" == '2' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<32: empty>', view, status, last_refresh_result, retry from refreshes order by view;
    drop table i;
    drop table j;"

# APPEND
$CLICKHOUSE_CLIENT -q "
    create materialized view k refresh every 10 year append (x Int64) engine Memory empty as select x*10 as x from src;
    select '<33: append>', * from k;
    system refresh view k;
    system wait view k;
    select '<34: append>', * from k;
    truncate table src;
    insert into src values (2), (3);
    system refresh view k;
    system wait view k;
    select '<35: append>', * from k order by x;"
# ALTER to non-APPEND
$CLICKHOUSE_CLIENT -q "
    alter table k modify refresh every 10 year;
    system wait view k;
    system refresh view k;
    system wait view k;
    select '<36: not append>', * from k order by x;
    drop table k;
    truncate table src;"

# APPEND + TO + regular materialized view reading from it.
$CLICKHOUSE_CLIENT -q "
    create table mid (x Int64) engine MergeTree order by x;
    create materialized view l refresh every 10 year append to mid empty as select x*10 as x from src;
    create materialized view m (x Int64) engine Memory as select x*10 as x from mid;
    insert into src values (1);
    system refresh view l;
    system wait view l;
    select '<37: append chain>', * from m;
    insert into src values (2);
    system refresh view l;
    system wait view l;
    select '<38: append chain>', * from m order by x;
    drop table l;
    drop table m;
    drop table mid;"

# Failing to create inner table.
$CLICKHOUSE_CLIENT -q "
    create materialized view n refresh every 1 second (x Int64) engine MergeTree as select 1 as x from numbers(2);" 2>/dev/null || echo "creating MergeTree without ORDER BY failed, as expected"
$CLICKHOUSE_CLIENT -q "
    create materialized view n refresh every 1 second (x Int64) engine MergeTree order by x as select 1 as x from numbers(2);
    drop table n;"

# Reading from table that doesn't exist yet.
$CLICKHOUSE_CLIENT -q "
    create materialized view o refresh every 1 second (x Int64) engine Memory as select x from nonexist; -- { serverError UNKNOWN_TABLE }
    create materialized view o (x Int64) engine Memory as select x from nonexist; -- { serverError UNKNOWN_TABLE }
    create materialized view o (x Int64) engine Memory as select x from nope.nonexist; -- { serverError UNKNOWN_DATABASE }
    create materialized view o refresh every 1 second (x Int64) engine Memory as select x from nope.nonexist settings allow_materialized_view_with_bad_select = 1;
    drop table o;"

$CLICKHOUSE_CLIENT -q "
    drop table refreshes;"
