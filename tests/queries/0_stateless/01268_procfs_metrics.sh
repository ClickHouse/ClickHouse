#!/usr/bin/env bash

# Sandbox does not provide CAP_NET_ADMIN capability but does have ProcFS mounted at /proc
# This ensures that OS metrics can be collected


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
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
            WHERE has(ProfileEvents.Names, 'OSCPUVirtualTimeMicroseconds') AND has(ProfileEvents.Names, 'OSReadChars')\
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

echo "Test OK"
