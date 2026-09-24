#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

check_roundtrip()
{
    query="$1"

    formatted=$(printf '%s\n' "$query" | $CLICKHOUSE_FORMAT) || exit 1
    formatted_again=$(printf '%s\n' "$formatted" | $CLICKHOUSE_FORMAT) || exit 1

    [ "$formatted" = "$formatted_again" ] || exit 1
}

check_roundtrip 'SELECT `cube`, count() FROM t GROUP BY `cube`'
check_roundtrip 'SELECT `rollup`, count() FROM t GROUP BY `rollup`'
check_roundtrip 'WITH `recursive` AS (SELECT 1 AS x) SELECT * FROM `recursive`'

echo "OK"
