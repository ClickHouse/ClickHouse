#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# local
prev="$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.errors WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' AND NOT remote")"
$CLICKHOUSE_CLIENT -q 'SELECT throwIf(1)' >& /dev/null
cur="$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.errors WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' AND NOT remote")"
echo local=$((cur - prev))

# remote
prev="$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.errors WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' AND remote")"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM remote('127.2', system.one) where throwIf(not dummy)" >& /dev/null
cur="$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.errors WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' AND remote")"
echo remote=$((cur - prev))
