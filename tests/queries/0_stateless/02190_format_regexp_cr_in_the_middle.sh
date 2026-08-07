#!/usr/bin/env bash
# Tags: no-fasttest, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo -ne 'xyz\rabc\nHello, world\r\nEnd' | ${CLICKHOUSE_LOCAL} --structure 's String' --input-format Regexp --format_regexp '(.*)' --query 'SELECT * FROM table'
