#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest - test requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="SELECT * FROM s3('s3://test/hello.csv', \
'ASIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY');"
extract_query() {
    ps -p "$1" -o args= | sed -E 's/.*--query(=|[[:space:]]+)//'
}
${CLICKHOUSE_CLIENT} --query "$QUERY" >/dev/null 2>&1 & pid=$!
sleep 0.5
extract_query "$pid"
kill -TERM "$pid" 2>/dev/null || :
wait  "$pid" 2>/dev/null || :


${CLICKHOUSE_CLIENT} --query="$QUERY" >/dev/null 2>&1 & pid=$!
sleep 0.5
extract_query "$pid"
kill -TERM "$pid" 2>/dev/null || :
wait  "$pid" 2>/dev/null || :
