#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select number, map(number, number) as map, 'Hello' as str from numbers(3) format ORC" | $CLICKHOUSE_LOCAL --input-format=ORC  -q "select * from table";

