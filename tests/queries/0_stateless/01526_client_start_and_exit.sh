#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Create a huge amount of tables, so Suggest will take a time to load
seq 1 1000 | sed -r -e 's/(.+)/CREATE TABLE IF NOT EXISTS test\1 (x UInt8) ENGINE = Memory;/' | ${CLICKHOUSE_CLIENT} -n

function stress()
{
    while true; do
        ./"$CURDIR"/01526_client_start_and_exit.expect
    done
}

export -f stress

for _ in {1..10}; do
    timeout 3 bash -c stress &
done

wait
echo 'Ok'

seq 1 1000 | sed -r -e 's/(.+)/DROP TABLE test\1;/' | ${CLICKHOUSE_CLIENT} -n
