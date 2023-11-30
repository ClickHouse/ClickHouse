#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nq "drop table if exists refreshes; drop table if exists a;"

$CLICKHOUSE_CLIENT -nq "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"


# Basic refreshing.
$CLICKHOUSE_CLIENT -nq "
    create materialized view a
        refresh after 1 second (x UInt64)
        engine Memory
        as select number from numbers(2) union all select rand64()"
$CLICKHOUSE_CLIENT -nq "select view, remaining_dependencies, exception, last_refresh_result in ('Unknown', 'Finished') from refreshes";
$CLICKHOUSE_CLIENT -nq "show create a" | sed "s/$CLICKHOUSE_DATABASE./<db>/"
# Wait for any refresh.
while [ "`$CLICKHOUSE_CLIENT -nq "select last_refresh_result from refreshes" | xargs`" == 'Unknown' ]
do
    sleep 0.1
done
# Check table contents.
$CLICKHOUSE_CLIENT -nq "select count(), sum(x=0), sum(x=1) from a"
# Wait for table contents to change.
res1="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values'`"
while :
do
    res2="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values'`"
    [ "$res2" == "$res1" ] || break
    sleep 0.1
done
time2="`$CLICKHOUSE_CLIENT -nq \"select reinterpret(now64(), 'Int64')\"`"
# Wait for another change.
while :
do
    res3="`$CLICKHOUSE_CLIENT -nq 'select * from a order by x format Values'`"
    [ "$res3" == "$res2" ] || break
    sleep 0.1
done
time3="`$CLICKHOUSE_CLIENT -nq \"select reinterpret(now64(), 'Int64')\"`"
# Check that the two changes were at least 500ms apart, in particular that we're not refreshing
# like crazy. This is potentially flaky, but we need at least one test that uses non-mocked timer
# to make sure the clock+timer code works at all. If it turns out flaky, increase refresh period above.
$CLICKHOUSE_CLIENT -nq "
    select min2($time3-$time2, 500);
    select next_refresh_time-last_refresh_time from refreshes;"

$CLICKHOUSE_CLIENT -nq "drop table refreshes; drop table a;"
