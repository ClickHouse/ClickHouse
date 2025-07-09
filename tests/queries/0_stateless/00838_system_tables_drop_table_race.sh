#!/usr/bin/env bash
# Tags: race

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS table"

seq 1 100 | sed -r -e "s/.+/CREATE TABLE table (x UInt8) ENGINE = MergeTree ORDER BY x; DROP TABLE table;/" | $CLICKHOUSE_CLIENT -n &
seq 1 100 | sed -r -e "s/.+/SELECT * FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' LIMIT 1000000, 1;/" | $CLICKHOUSE_CLIENT -n 2>/dev/null &

wait
