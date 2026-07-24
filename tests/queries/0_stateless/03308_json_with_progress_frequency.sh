#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Usually not more than one progress event per 100 ms:
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress&max_execution_time=1&interactive_delay=100000" -d "SELECT count() FROM system.numbers" | 
    grep -F '"progress"' | wc -l | ${CLICKHOUSE_LOCAL} --input-format TSV --query "SELECT c1 < 20 FROM table"
