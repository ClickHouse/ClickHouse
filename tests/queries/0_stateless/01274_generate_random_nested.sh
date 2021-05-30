#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "SELECT * FROM generateRandom('\"ParsedParams.Key1\" Array(String), \"ParsedParams.Key2\" Array(Float64), x String', 1, 10, 2) LIMIT 10" > /dev/null;
