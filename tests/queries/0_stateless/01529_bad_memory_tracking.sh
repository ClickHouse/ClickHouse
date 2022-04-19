#!/usr/bin/env bash
# Tags: no-replicated-database

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} --max_memory_usage '10G' --query "SELECT i FROM generateRandom('i Array(Int8)', 1, 1, 1048577) LIMIT 65536" |& grep -v -e 'Received exception from server' -e 'Code: 241' -e '(query: '
done
exit 0
