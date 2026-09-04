#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: `system.errors` keeps only the latest query ID for each error code.

# Comparing a `DateTime64` column against a `DateTime` constant inside an `AND` made
# `LogicalExpressionOptimizerPass` round-trip the constant through the column's type to check the
# fold was lossless. The reverse leg of that round trip was not implemented, so it threw
# `TYPE_MISMATCH` on every evaluation. The exception was swallowed and the query was correct, but
# `system.errors` counted it, and only a restart cleared the counter.
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

# A sub-second constant is not representable as DateTime, so the round trip must reject it rather
# than truncate: the row at exactly 00:00:00 must not be matched by a bound of 00:00:00.5.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_dt64_cmp
    WHERE A = 'x' AND Timestamp >= toDateTime64('2020-01-01 00:00:00.5', 9)"

# The same narrowing exists for the time-of-day pair, and `enable_time_time64_type` is on by default.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_t64_cmp"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_t64_cmp (k UInt8, T Time64(9)) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_t64_cmp VALUES (1, '12:00:00'), (2, '13:00:00')"

time_query_id="05076_t64_${CLICKHOUSE_DATABASE}_${RANDOM}"
$CLICKHOUSE_CLIENT --query_id="${time_query_id}" -q "
    SELECT count() FROM t_t64_cmp WHERE k = 1 AND T >= toTime('11:00:00')"
$CLICKHOUSE_CLIENT -q "
    SELECT count() = 0 FROM system.errors
    WHERE name = 'TYPE_MISMATCH' AND query_id = '${time_query_id}'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_t64_cmp"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_dt64_cmp"
