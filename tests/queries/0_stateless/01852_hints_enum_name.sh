#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT CAST('Helo', 'Enum(\'Hello\' = 1, \'World\' = 2)')" 2>&1 | grep -q "may be you meant: \['Hello'\]" && echo 'OK' || echo 'FAIL'

