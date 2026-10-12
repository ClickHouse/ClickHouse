#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Parquet`, `Arrow` and `ORC` formats are not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The columnar formats (`Parquet`, `Arrow`, `ORC`) accept a numeric source column for an `IPv4`
# destination: `Parquet` / `Arrow` cast the decoded column to the requested type (valid for the
# `UInt32`-backed `IPv4`) and `ORC` has an explicit `Int32` -> `IPv4` read path. So when a row
# fails on an unrelated column, an inferred numeric type going into an `IPv4` column must NOT be
# flagged as a structure mismatch for these formats.
#
# In the first three cases below the column `ip` holds a valid numeric value and the column `u`
# holds a string that is not a valid `UUID`, which fails with a genuine parse error
# (`CANNOT_PARSE_UUID`), triggering the diagnostic. An inferred `String` going into a `UUID`
# destination is compatible (the value is re-parsed from the string), so the only candidate for a
# false positive is the numeric `ip` going into the `IPv4` column. The last case shows the
# boundary: `IPv6` is not backed by an integer, so a numeric source column is still rejected
# there — with a conversion error rather than a parse error, so the insert fails without
# involving the diagnostic at all.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_PARQUET=$CLICKHOUSE_TMP/data_04760.parquet
DATA_ARROW=$CLICKHOUSE_TMP/data_04760.arrow
DATA_ORC=$CLICKHOUSE_TMP/data_04760.orc

$CLICKHOUSE_LOCAL -q "SELECT 16909060::UInt32 AS ip, 'not-a-uuid' AS u FORMAT Parquet" > "$DATA_PARQUET"
$CLICKHOUSE_LOCAL -q "SELECT 16909060::UInt32 AS ip, 'not-a-uuid' AS u FORMAT Arrow" > "$DATA_ARROW"
$CLICKHOUSE_LOCAL -q "SELECT 16909060::UInt32 AS ip, 'not-a-uuid' AS u FORMAT ORC" > "$DATA_ORC"

echo "-- Parquet: a numeric value for an IPv4 column is valid (no false positive)"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT Parquet"
    cat "$DATA_PARQUET"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- Arrow: a numeric value for an IPv4 column is valid (no false positive)"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT Arrow"
    cat "$DATA_ARROW"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- ORC: a numeric value for an IPv4 column is valid (no false positive)"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT ORC"
    cat "$DATA_ORC"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- Parquet: a numeric value is still rejected for an IPv6 column (a conversion error, not a parse error)"
DATA_IP_ONLY=$CLICKHOUSE_TMP/data_04760_ip_only.parquet
$CLICKHOUSE_LOCAL -q "SELECT 16909060::UInt32 AS ip FORMAT Parquet" > "$DATA_IP_ONLY"
{
    echo "CREATE TABLE t (ip IPv6) ENGINE = Memory; INSERT INTO t FORMAT Parquet"
    cat "$DATA_IP_ONLY"
} | $CLICKHOUSE_LOCAL 2>&1 | {
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

rm -f "$DATA_PARQUET" "$DATA_ARROW" "$DATA_ORC" "$DATA_IP_ONLY"
