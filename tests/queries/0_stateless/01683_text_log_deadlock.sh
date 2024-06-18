#!/usr/bin/env bash
# Tags: deadlock

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_BENCHMARK --secure -i 5000 -c 32 --query 'SELECT 1' 2>&1 | grep -oF 'queries: 5000'
