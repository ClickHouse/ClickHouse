#!/usr/bin/env bash
# Tags: memory-engine, atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An absurd RANDOMIZE FOR caps the offset at 3e9 seconds. The first timeslot is 2170-01-01, so both
# signs of the offset stay in the future and the view is scheduled without refreshing.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv refresh every 200 year randomize for 10000000000000000000 second
        (x Int64) engine Memory as select 1 as x;"

# next_refresh_time is a UInt32 DateTime, so the expected instants are compared modulo 2^32.
query="
    with toInt64(toDateTime64('2170-01-01 00:00:00', 0, 'UTC')) as slot
    select status = 'Scheduled' and last_refresh_time is null
        and toUInt32(next_refresh_time) in ((slot - 3000000000) % 4294967296, (slot + 3000000000) % 4294967296)
    from system.view_refreshes where database = currentDatabase() and view = 'rmv'"

for _ in {1..100}; do
    [ "$($CLICKHOUSE_CLIENT -q "$query")" = 1 ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "$query"
