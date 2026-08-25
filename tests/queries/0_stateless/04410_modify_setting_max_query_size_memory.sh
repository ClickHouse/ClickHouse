#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/79887
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"

cols=$($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(arrayMap(i -> 'c' || toString(i) || ' Int8', range(200)), ', ')")
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (${cols}) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT * FROM generateRandom('${cols}', 1, 10, 1) LIMIT 10"

# MODIFY SETTING must be rejected — and must not have already deleted rows.
$CLICKHOUSE_CLIENT --max_query_size=1024 -q "ALTER TABLE t MODIFY SETTING max_rows_to_keep=5" 2>&1 | grep -o -F -m1 "QUERY_IS_TOO_LARGE"

# Row count must still be 10 — the rejected ALTER must not have trimmed data.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
