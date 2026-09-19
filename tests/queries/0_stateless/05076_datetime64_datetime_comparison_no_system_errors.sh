#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: `system.errors` keeps only the latest query ID for each error code.

# A `DateTime64` column compared with a `DateTime` constant inside `AND` made the optimizer's
# round-trip conversion throw `TYPE_MISMATCH` on every query, and `system.errors` counted it.
# https://github.com/ClickHouse/ClickHouse/issues/117903

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

query_id="05076_dt64_${CLICKHOUSE_DATABASE}_${RANDOM}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dt64_cmp"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_dt64_cmp (A String, Timestamp DateTime64(9))
    ENGINE = MergeTree ORDER BY (A, Timestamp)"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_dt64_cmp VALUES ('x', '2020-01-01 00:00:00'), ('y', '2020-01-02 00:00:00')"

$CLICKHOUSE_CLIENT --query_id="${query_id}" -q "
    SELECT count() FROM t_dt64_cmp
    WHERE A = 'x' AND Timestamp >= toDateTime('2019-01-01 00:00:00')"

# The query must not have recorded a TYPE_MISMATCH of its own.
$CLICKHOUSE_CLIENT -q "
    SELECT count() = 0 FROM system.errors
    WHERE name = 'TYPE_MISMATCH' AND query_id = '${query_id}'"

# A DateTime constant is representable as DateTime64, so the fold applies and the result is right.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_dt64_cmp
    WHERE A = 'x' AND Timestamp >= toDateTime('2019-01-01 00:00:00') AND Timestamp >= toDateTime('2018-01-01 00:00:00')"

# A sub-second bound is not folded, so the row at 00:00:00 must not match 00:00:00.5.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_dt64_cmp
    WHERE A = 'x' AND Timestamp >= toDateTime64('2020-01-01 00:00:00.5', 9)"

# The same constant materialized as a value is truncated like CAST.
$CLICKHOUSE_CLIENT -q "SELECT x FROM values('x DateTime(\'UTC\')', toDateTime64('2020-01-01 00:00:00.5', 1, 'UTC'))"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dt64_cmp"
