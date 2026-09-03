#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress&max_execution_time=1&max_rows_to_read=0&http_write_exception_in_output_format=1" -d "SELECT count() FROM system.numbers" 2>&1)
RES=$(echo "$OUT" | grep -F '"exception"' | grep -o -F '{"exception":"Code: 159. DB::Exception: Timeout exceeded' | sed -r -e 's/xception/xpected/g')

echo "$RES"

if [ -z "$RES" ]
then
    echo "$OUT"
fi
