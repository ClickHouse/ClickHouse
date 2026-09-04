#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Query: select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.1234567', 'yyyy-MM-dd HH:mm:ss.SSSSSSS', 'UTC')"
echo "Expected error: Precision 7 is invalid (must be \[0, 6\])"
echo "Result:"
output=$(${CLICKHOUSE_CLIENT} -q "select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.1234567', 'yyyy-MM-dd HH:mm:ss.SSSSSSS', 'UTC')" 2>&1)
echo "$output" | grep -Fq 'Precision 7 is invalid (must be [0, 6])' && echo "OK" || echo "FAIL"
