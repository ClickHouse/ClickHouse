#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo '20210203,2021-03-04,20210405' | $CLICKHOUSE_LOCAL --input-format CSV --structure 'a Date, b Date, c Date' --query 'SELECT * FROM table'
