#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In previous versions, when there is a subquery, we start writing the progress (accumulated from processing the subquery)
# during initialization of data format, which was before HTTP headers were written. 
# This was leading to losing of HTTP headers, such as X-ClickHouse-QueryId and X-ClickHouse-Format, as well as Content-Type.
${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress" -d "SELECT (SELECT 1)" 2>&1 | grep -F 'X-ClickHouse-Format: '
