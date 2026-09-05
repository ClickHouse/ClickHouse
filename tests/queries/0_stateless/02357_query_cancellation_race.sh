#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table tab (x UInt64, y String) engine = MergeTree order by x"
for _ in $(seq 1 100); do timeout -s 2 0.05 $CLICKHOUSE_CLIENT --interactive_delay 1000 -q "insert into tab select number, toString(number) from system.numbers" || true; done
