#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -v -sS "${CLICKHOUSE_URL}&http_write_exception_in_output_format=1" -d "SELECT * FROM nonexistent.nonexistent FORMAT JSONLines" 2>&1 | grep -P 'X-ClickHouse-Format: |X-ClickHouse-Exception-Code: |"exception": |< HTTP/1\.1 ' | sed -r -e 's/\(version .+\)//'
