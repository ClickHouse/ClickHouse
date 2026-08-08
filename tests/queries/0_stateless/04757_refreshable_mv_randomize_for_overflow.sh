#!/usr/bin/env bash
# Tags: memory-engine, atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A clock-unit window reaches the offset through the seconds term of
# CalendarTimeInterval::minSeconds and a calendar-unit one through its months term, so each term
# needs its own view.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_seconds refresh every 1 hour randomize for 10000000000000000000 second
        (x Int64) engine Memory as select 1 as x;
    create materialized view rmv_months refresh every 1 hour randomize for 1000000000000000000 year
        (x Int64) engine Memory as select 1 as x;"

# Poll for the endpoint itself: until scheduling runs the column holds the epoch, which is neither
# NULL nor an endpoint. Bounded, so the loop also ends without a live server.
for _ in {1..100}; do
    [ "$($CLICKHOUSE_CLIENT -q "select countIf(toUInt32(next_refresh_time) in (2077252342, 2217714954)) from system.view_refreshes where database = currentDatabase()" 2>/dev/null)" = 2 ] && break
    sleep 0.1
done

# The two constants are the saturated ends reduced to whole seconds modulo the UInt32 of DateTime.
# A NULL would mean time_point::max(), the sentinel that makes a view wait forever.
$CLICKHOUSE_CLIENT -q "
    select view, toUInt32(next_refresh_time) in (2077252342, 2217714954)
    from system.view_refreshes where database = currentDatabase() order by view"
