#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "select * from (select 1 as x union all select 1 union distinct select 3)" | $CLICKHOUSE_FORMAT --backslash;

echo "select 1 union all (select 1 union distinct select 1)" | $CLICKHOUSE_FORMAT --backslash;
