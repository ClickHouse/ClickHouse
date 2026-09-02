#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo -ne "True\r\nFalse\r\n" | $CLICKHOUSE_LOCAL --structure='x Bool' --input-format=CSV -q "select * from table";
