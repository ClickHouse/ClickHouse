#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

echo "
	DROP TABLE IF EXISTS storage_join_race;
	CREATE TABLE storage_join_race (x UInt64, y UInt64) Engine = Join(ALL, FULL, x);
" | $CLICKHOUSE_CLIENT

function read_thread_big()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        echo "
            SELECT * FROM ( SELECT number AS x FROM numbers(100000) ) AS t1 ALL FULL JOIN storage_join_race USING (x) FORMAT Null;
        " | $CLICKHOUSE_CLIENT
    done
}

function read_thread_small()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        echo "
            SELECT * FROM ( SELECT number AS x FROM numbers(10) ) AS t1 ALL FULL JOIN storage_join_race USING (x) FORMAT Null;
        " | $CLICKHOUSE_CLIENT
    done
}

function read_thread_select()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        echo "
            SELECT * FROM storage_join_race FORMAT Null;
        " | $CLICKHOUSE_CLIENT
    done
}

TIMEOUT=20

read_thread_big 2> /dev/null &
read_thread_small 2> /dev/null &
read_thread_select 2> /dev/null &

# Run insert query with a sleep to make sure that it is executed all the time during the read queries.
echo "
    INSERT INTO storage_join_race
        SELECT number AS x, sleepEachRow(0.1) + number AS y FROM numbers ($TIMEOUT * 10)
        SETTINGS function_sleep_max_microseconds_per_block = 100000000, max_block_size = 10;
" | $CLICKHOUSE_CLIENT

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE storage_join_race"
