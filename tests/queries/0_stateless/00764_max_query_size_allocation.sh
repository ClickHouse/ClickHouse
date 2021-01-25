#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_query_size=1000000000&max_memory_usage=10000000" -d "SELECT 1"
