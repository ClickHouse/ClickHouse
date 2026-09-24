#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

output=$(echo 'SELECT `cube`, count() FROM t GROUP BY `cube`' | $CLICKHOUSE_FORMAT)
echo "$output" | grep -Fq 'GROUP BY `cube`' || exit 1
echo "cube OK"

output=$(echo 'SELECT `rollup`, count() FROM t GROUP BY `rollup`' | $CLICKHOUSE_FORMAT)
echo "$output" | grep -Fq 'GROUP BY `rollup`' || exit 1
echo "rollup OK"

output=$(echo 'WITH `recursive` AS (SELECT 1 AS x) SELECT * FROM `recursive`' | $CLICKHOUSE_FORMAT)
echo "$output" | grep -Fq '`recursive` AS' || exit 1
echo "$output" | grep -Fq 'FROM `recursive`' || exit 1
echo "recursive OK"
