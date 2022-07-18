#!/usr/bin/env bash

# Sandbox does not provide CAP_NET_ADMIN capability but does have ProcFS mounted at /proc
# This ensures that OS metrics can be collected


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function read_numbers_func()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT * FROM numbers(600000000) FORMAT Null SETTINGS max_threads = 1
    ";
}


function show_processes_func()
{
    while true; do
        sleep 0.1;

        # These two system metrics for the generating query above are guaranteed to be nonzero when ProcFS is mounted at /proc
        $CLICKHOUSE_CLIENT -q "
            SELECT count() > 0 FROM system.processes\
            WHERE ProfileEvents['OSCPUVirtualTimeMicroseconds'] > 0 AND ProfileEvents['OSReadChars'] > 0 \
            SETTINGS max_threads = 1
        " | grep '1' && break;
    done
}


export -f read_numbers_func;
export -f show_processes_func;

TIMEOUT=3

timeout $TIMEOUT bash -c read_numbers_func &
timeout $TIMEOUT bash -c show_processes_func &

wait

# otherwise it can be alive after test
query_alive=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query ILIKE 'SELECT * FROM numbers(600000000)%'")
while [[ $query_alive != 0 ]]
do
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query ilike '%SELECT * FROM numbers(600000000)%'" 2> /dev/null 1> /dev/null
    sleep 0.5
    query_alive=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query ILIKE 'SELECT * FROM numbers(600000000)%'")
done

echo "Test OK"
