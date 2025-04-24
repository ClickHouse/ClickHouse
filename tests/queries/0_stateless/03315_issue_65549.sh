#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="SELECT * FROM s3('s3://test/hello.csv', 'ASIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY');"

${CLICKHOUSE_CLIENT} --query "${QUERY}" &
pid=$!

sleep 1

ps -p "$pid" -o args=


QUERY="SELECT * FROM s3('s3://test/hello.csv', 'ASIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY');"

${CLICKHOUSE_CLIENT} --query="${QUERY}" &
pid=$!

sleep 1

ps -p "$pid" -o args=
