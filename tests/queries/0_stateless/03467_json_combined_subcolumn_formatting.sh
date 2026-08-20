#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --query 'select json.@a from test';
$CLICKHOUSE_FORMAT --query 'select json.@a.b.c from test';
$CLICKHOUSE_FORMAT --query 'select json.@`s^b`.x.y from test';
$CLICKHOUSE_FORMAT --query 'select json.`$a` from test';
$CLICKHOUSE_FORMAT --query 'select json.path.@sub from test';
