#!/usr/bin/env bash
# Verify that clickhouse-local reports accurate rows_read/bytes_read in JSON statistics,
# including progress from scalar subqueries evaluated during analysis.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "#1 simple query"
${CLICKHOUSE_LOCAL} --query="SELECT count() FROM numbers(100) FORMAT JSON" | grep -o '"rows_read": [0-9]*'

echo "#2 scalar subquery"
${CLICKHOUSE_LOCAL} --query="SELECT (SELECT count() FROM numbers(100000)) SETTINGS enable_analyzer = 1 FORMAT JSON" | grep -o '"rows_read": [0-9]*'
