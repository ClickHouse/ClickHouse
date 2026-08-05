#!/usr/bin/env bash
# Tags: race, no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q """
DROP TABLE IF EXISTS t1_02867;
CREATE TABLE t1_02867 (x UInt64) ENGINE=Set();
"""

function repeat_select() {
    n=0
    while [ "$n" -lt 20 ];
    do
        n=$(( n + 1 ))
        $CLICKHOUSE_CLIENT -q "SELECT count() as a FROM numbers(10) WHERE number IN t1_02867" > /dev/null 2> /dev/null || exit
    done
}

function repeat_truncate_insert() {
    n=0
    while [ "$n" -lt 20 ];
    do
        n=$(( n + 1 ))
        $CLICKHOUSE_CLIENT -q "TRUNCATE t1_02867;" > /dev/null 2> /dev/null || exit
    done
}

repeat_select &
repeat_truncate_insert &
repeat_select &
repeat_truncate_insert &
repeat_select &
repeat_truncate_insert &
repeat_select &
repeat_truncate_insert &

sleep 10

$CLICKHOUSE_CLIENT -m -q "DROP TABLE IF EXISTS t1_02867;"
