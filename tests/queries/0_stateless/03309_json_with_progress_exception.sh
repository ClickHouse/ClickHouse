#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress&max_execution_time=1" -d "SELECT count() FROM system.numbers" |
    grep -F '"exception"' | grep -o -F '{"exception":"Code: 159. DB::Exception: Timeout exceeded: elapsed' | sed -r -e 's/xception/xpected/g'
