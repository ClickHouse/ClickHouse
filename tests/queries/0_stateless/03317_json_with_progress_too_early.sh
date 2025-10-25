#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In previous versions in presence of a subquery, we started writing the progress data (accumulated from processing the subquery)
# during initialization of the data format, which happened ahead of writing HTTP headers. 
# This could led to the loss of HTTP headers, such as X-ClickHouse-QueryId and X-ClickHouse-Format, as well as Content-Type.
${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress" -d "SELECT (SELECT 1)" 2>&1 | grep -F 'X-ClickHouse-Format: '
