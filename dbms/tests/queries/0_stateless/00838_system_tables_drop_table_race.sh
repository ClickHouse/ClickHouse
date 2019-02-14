#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.table"

seq 1 100 | sed -r -e "s/.+/CREATE TABLE test.table (x UInt8) ENGINE = MergeTree ORDER BY x; DROP TABLE test.table;/" | $CLICKHOUSE_CLIENT -n &
seq 1 100 | sed -r -e "s/.+/SELECT * FROM system.tables WHERE database = 'test' LIMIT 1000000, 1;/" | $CLICKHOUSE_CLIENT -n &

wait
