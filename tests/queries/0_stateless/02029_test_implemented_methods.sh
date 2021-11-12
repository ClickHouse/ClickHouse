#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CURL "${CLICKHOUSE_URL}" -X GET -d "SELECT 1" -vs 2>&1 | grep "OK"
$CLICKHOUSE_CURL "${CLICKHOUSE_URL}" -X aaa -vs 2>&1 | grep "Not Implemented"
$CLICKHOUSE_CURL "${CLICKHOUSE_URL}" -d "SELECT 1" -vs 2>&1 | grep "OK"
