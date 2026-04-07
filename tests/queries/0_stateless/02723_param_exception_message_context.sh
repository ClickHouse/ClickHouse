#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The exception message about unparsed parameter also tells about the name of the parameter.
$CLICKHOUSE_CLIENT --param_x Hello --query "SELECT {x:Array(String)}" 2>&1 | rg -oF "for query parameter 'x'" | uniq
