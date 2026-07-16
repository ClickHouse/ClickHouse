#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ch_curl() {
  ${CLICKHOUSE_CURL} -sS "$@" 2> >(grep -v "transfer closed with outstanding read data remaining" >&2)
  code=$?
  # Treat "curl: (18) transfer closed with outstanding read data remaining" as success
  [ "$code" -eq 18 ] && return 0
  return "$code"
}

# Usually not more than one progress event per 100 ms:
response=$(ch_curl "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress&max_execution_time=1&interactive_delay=100000" -d "SELECT count() FROM system.numbers")
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "unknown error, response: $response"
  exit $rc
fi

echo "$response" | grep -F '"progress"' | wc -l | ${CLICKHOUSE_LOCAL} --input-format TSV --query "SELECT c1 < 20 FROM table"
