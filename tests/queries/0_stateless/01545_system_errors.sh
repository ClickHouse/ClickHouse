#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

prev="$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.errors WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'")"
$CLICKHOUSE_CLIENT -q 'SELECT throwIf(1)' >& /dev/null
cur="$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.errors WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'")"
echo $((cur - prev))
