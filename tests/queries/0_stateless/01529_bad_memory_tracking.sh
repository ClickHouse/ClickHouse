#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} --max_memory_usage '10G' --query "SELECT i FROM generateRandom('i Array(Int8)', 1, 1, 1048577) LIMIT 65536" 2>&1 | grep -v -P '^(Received exception from server|Code: 241)' ||:
done
