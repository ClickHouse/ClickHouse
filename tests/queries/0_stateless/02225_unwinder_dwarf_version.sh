#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# In case if DWARF-5 debug info is generated during build, it cannot parse it and correctly show this exception. This is why we limit DWARF version to 4 max.
$CLICKHOUSE_LOCAL --query "SELECT throwIf(1)" 2>&1 | grep -c 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'
