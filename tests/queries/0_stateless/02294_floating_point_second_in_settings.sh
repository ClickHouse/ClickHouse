#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

MAX_TIMEOUT_MS=1100
MAX_TIMEOUT_SEC=$(awk "BEGIN {printf \"%.3f\", ${MAX_TIMEOUT_MS}/1000}") # Use 1.1 because using 0.x truncates to 0 in older releases

# Function to check the output for maximum execution time in milliseconds
function check_output() {
  # Extract the maximum time used from the output, including 'ms'
  MAXTIME_USED=$(echo "$1" | grep -Eo "maximum: [0-9]+(\.[0-9]+)? ms" | head -n1 || true)
  EXPECTED="maximum: ${MAX_TIMEOUT_MS} ms"
  if [ "${MAXTIME_USED}" != "${EXPECTED}" ]; then
    echo "'${MAXTIME_USED}' is not equal to '${EXPECTED}'"
    echo "OUTPUT: $1"
  else
    echo "$MAXTIME_USED"
  fi
}

# TCP CLIENT
echo "TCP CLIENT"
OUTPUT=$($CLICKHOUSE_CLIENT --max_rows_to_read 0 --max_execution_time $MAX_TIMEOUT_SEC -q "SELECT count() FROM system.numbers" 2>&1 || true)
check_output "${OUTPUT}"

echo "TCP CLIENT WITH SETTINGS IN QUERY"
OUTPUT=$($CLICKHOUSE_CLIENT --max_rows_to_read 0 -q "SELECT count() FROM system.numbers SETTINGS max_execution_time=${MAX_TIMEOUT_SEC}" 2>&1 || true)
check_output "${OUTPUT}"

# HTTP CLIENT
echo "HTTP CLIENT"
OUTPUT=$(${CLICKHOUSE_CURL_COMMAND} -q -sS "$CLICKHOUSE_URL&max_execution_time=${MAX_TIMEOUT_SEC}&max_rows_to_read=0" -d "SELECT count() FROM system.numbers" || true)
check_output "${OUTPUT}"

# CHECK system.settings
echo "TABLE: system.settings"
echo "SELECT name, value, changed FROM system.settings WHERE name = 'max_execution_time'" | $CLICKHOUSE_CLIENT_BINARY --max_execution_time 30.5
