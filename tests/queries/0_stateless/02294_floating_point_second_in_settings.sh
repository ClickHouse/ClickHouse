#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail


MAX_TIMEOUT=1.1 # Use 1.1 because using 0.x truncates to 0 in older releases

function check_output() {
  MAXTIME_USED=$(echo "$1" | grep -Eo "maximum: [0-9]+\.[0-9]+" | head -n1 || true)
  if [ "${MAXTIME_USED}" != "maximum: ${MAX_TIMEOUT}" ];
  then
    echo "'$MAXTIME_USED' is not equal to 'maximum: ${MAX_TIMEOUT}'"
    echo "OUTPUT: $1"
  else
    echo "$MAXTIME_USED"
  fi
}

# TCP CLIENT
echo "TCP CLIENT"
OUTPUT=$($CLICKHOUSE_CLIENT --max_rows_to_read 0 --max_execution_time $MAX_TIMEOUT -q "SELECT count() FROM system.numbers" 2>&1 || true)
check_output "${OUTPUT}"

echo "TCP CLIENT WITH SETTINGS IN QUERY"
OUTPUT=$($CLICKHOUSE_CLIENT --max_rows_to_read 0 -q "SELECT count() FROM system.numbers SETTINGS max_execution_time=$MAX_TIMEOUT" 2>&1 || true)
check_output "${OUTPUT}"

# HTTP CLIENT
echo "HTTP CLIENT"
OUTPUT=$(${CLICKHOUSE_CURL_COMMAND} -q -sS "$CLICKHOUSE_URL&max_execution_time=${MAX_TIMEOUT}&max_rows_to_read=0" -d \
    "SELECT count() FROM system.numbers" || true)
check_output "${OUTPUT}"

# CHECK system.settings
echo "TABLE: system.settings"
echo "SELECT name, value, changed from system.settings where name = 'max_execution_time'" | $CLICKHOUSE_CLIENT_BINARY --max_execution_time 30.5
