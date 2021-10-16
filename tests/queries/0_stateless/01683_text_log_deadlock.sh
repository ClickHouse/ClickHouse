#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_BENCHMARK --secure -i 25000 -c 32 --query 'SELECT 1' 2>&1 | grep -oF 'queries 25000'
