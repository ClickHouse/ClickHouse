#!/usr/bin/env bash
# Ref: https://github.com/ClickHouse/ClickHouse/issues/1576
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "#1"
${CLICKHOUSE_CLIENT} --query='SELECT count() FROM numbers(100) FORMAT JSON;' | grep -a -v "elapsed"
echo "#2"
${CLICKHOUSE_CLIENT} --query='SELECT (SELECT max(number), count(number) FROM numbers(100000) as n) SETTINGS max_block_size = 65505 FORMAT JSON;' | grep -a -v "elapsed" | grep -v "_subquery"
