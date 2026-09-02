#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT CAST('Helo' AS Enum('Hello' = 1, 'World' = 2))" 2>&1 | grep -q -F "maybe you meant: ['Hello']" && echo 'OK' || echo 'FAIL'

