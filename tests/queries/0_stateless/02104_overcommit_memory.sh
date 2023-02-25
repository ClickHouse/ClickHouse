#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q 'CREATE USER IF NOT EXISTS u1 IDENTIFIED WITH no_password'
$CLICKHOUSE_CLIENT -q 'GRANT ALL ON *.* TO u1'

function overcommited()
{
    while true; do
        $CLICKHOUSE_CLIENT -u u1 -q 'SELECT number FROM numbers(130000) GROUP BY number SETTINGS max_guaranteed_memory_usage=1,memory_usage_overcommit_max_wait_microseconds=500' 2>&1 | grep -F -q "MEMORY_LIMIT_EXCEEDED" && echo "OVERCOMMITED WITH USER LIMIT IS KILLED"
    done
}

function expect_execution()
{
    while true; do
        $CLICKHOUSE_CLIENT -u u1 -q 'SELECT number FROM numbers(130000) GROUP BY number SETTINGS max_memory_usage_for_user=5000000,max_guaranteed_memory_usage=2,memory_usage_overcommit_max_wait_microseconds=500' >/dev/null 2>/dev/null
    done
}

export -f overcommited
export -f expect_execution

function user_test()
{
    for _ in {1..10};
    do
        timeout 10 bash -c overcommited &
        timeout 10 bash -c expect_execution &
    done;

    wait
}

output=$(user_test)

if test -z "$output"
then
    echo "OVERCOMMITED WITH USER LIMIT WAS NOT KILLED"
else
    echo "OVERCOMMITED WITH USER LIMIT WAS KILLED"
fi

$CLICKHOUSE_CLIENT -q 'DROP USER IF EXISTS u1'
