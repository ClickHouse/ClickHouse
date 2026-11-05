#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/79887
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"

cols=$($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(arrayMap(i -> 'c' || toString(i) || ' Int8', range(200)), ', ')")
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (${cols}) ENGINE = MergeTree ORDER BY tuple()"

# MODIFY COMMENT must be rejected — and must not leave in-memory metadata inconsistent.
$CLICKHOUSE_CLIENT --max_query_size=1024 -q "ALTER TABLE t MODIFY COMMENT 'x'" 2>&1 | grep -o -F -m1 "QUERY_IS_TOO_LARGE"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't' AND comment != ''"

$CLICKHOUSE_CLIENT -q "DETACH TABLE t"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
