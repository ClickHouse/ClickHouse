#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

cur_name=${BASH_SOURCE[0]}

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.json_parse;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.json_parse (aaa String, bbb String) ENGINE = Memory;"

for n in {1..1000000}; do echo '{"aaa":"aaa","bbb":"bbb"}'; done | curl -sS "${CLICKHOUSE_URL}?query=INSERT%20INTO%20test.json_parse%20FORMAT%20JSONEachRow" -0 --data-binary @-

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.json_parse;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.json_parse;"
