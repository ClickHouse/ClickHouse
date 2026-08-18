#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Match the bracketed priority marker anywhere: the log prefix omits host_name/query_id
# when empty, so the marker column is not fixed and a positional awk '{print $8}' is fragile.
# head -n 1 keeps the pipeline exit code 0 even when a negative check matches nothing.
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="trace" --query="SELECT 1" 2>&1 | grep -oE '<Trace>' | head -n 1
echo "."
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="debug" --query="SELECT 1" 2>&1 | grep -oE '<Debug>' | head -n 1
echo "."
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="information" --query="SELECT 1" 2>&1 | grep -oE '<Information>' | head -n 1
echo "."
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="error" --query="SELECT throwIf(1)" 2>&1 | grep -oE '<Error>' | head -n 1
echo "-"
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="debug" --query="SELECT 1" 2>&1 | grep -oE '<Trace>' | head -n 1
echo "."
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="information" --query="SELECT 1" 2>&1 | grep -oE '<Debug>|<Trace>' | head -n 1
echo "."
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="error" --query="SELECT throwIf(1)" 2>&1 | grep -oE '<Debug>|<Trace>|<Information>' | head -n 1
echo "."
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="None" --query="SELECT throwIf(1)" 2>&1 | grep -oE '<Debug>|<Trace>|<Information>|<Error>' | head -n 1
