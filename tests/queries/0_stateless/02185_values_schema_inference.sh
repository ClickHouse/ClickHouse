#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "select * from values('String', 'abc', 'def')"
$CLICKHOUSE_CLIENT -q "select * from values(1, -1, 10000, -10000, 1000000)"
$CLICKHOUSE_CLIENT -q "select * from values((1, 'string', [1, 2, -1]), (-10, 'def', [10, 20, 10000]))"
$CLICKHOUSE_CLIENT -q "select * from values((1, NULL, [1, 2, -1]), (NULL, 'def', [10, NULL, 10000]))"
$CLICKHOUSE_CLIENT -q "select * from values(((1, '1'), 10), ((-1, '-1'), 1000000))"

