#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="SELECT * FROM s3('http://localhost:9000/test/hello.csv', 'secret', 'secret', 'CSV', 'value String');"

${CLICKHOUSE_CLIENT} --query "${QUERY}" &
pid=$!

sleep 1

ps -p "$pid" -o args=


QUERY="SELECT * FROM s3('http://localhost:9000/test/hello.csv', 'secret', 'secret', 'CSV', 'value String');"

${CLICKHOUSE_CLIENT} --query="${QUERY}" &
pid=$!

sleep 1

ps -p "$pid" -o args=
