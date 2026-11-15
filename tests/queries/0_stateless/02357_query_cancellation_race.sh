#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table tab (x UInt64, y String) engine = MergeTree order by x"
# The source is unbounded on purpose: only the SIGINT below may end this insert, never a read limit.
for _ in $(seq 1 100); do timeout -s 2 --kill-after=5 0.05 $CLICKHOUSE_CLIENT --interactive_delay 1000 -q "insert into tab select number, toString(number) from system.numbers settings max_rows_to_read = 0, max_bytes_to_read = 0" || true; done
