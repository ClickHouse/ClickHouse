#!/usr/bin/env bash
# Tags: no-parallel

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# No log lines without query id
$CLICKHOUSE_CLIENT --query_id=hello_00971 --query="SELECT count() FROM numbers(10)" 2>&1 | grep -vF ' {hello_00971} ' | grep -P '<\w+>' ||:
