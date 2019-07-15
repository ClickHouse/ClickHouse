#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

# No log lines without query id
$CLICKHOUSE_CLIENT --send_logs_level=trace --query_id=hello --query="SELECT count() FROM numbers(10)" 2>&1 | grep -vF ' {hello} ' | grep -P '<\w+>'
