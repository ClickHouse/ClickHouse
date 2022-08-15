#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS json_parse;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE json_parse (aaa String, bbb String) ENGINE = Memory;"

for _ in {1..1000000}; do echo '{"aaa":"aaa","bbb":"bbb"}'; done | curl -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20json_parse%20FORMAT%20JSONEachRow" -0 --data-binary @-

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM json_parse;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE json_parse;"
