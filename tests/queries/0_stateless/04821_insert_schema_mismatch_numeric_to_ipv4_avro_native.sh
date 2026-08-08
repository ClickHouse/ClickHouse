#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Avro` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Avro` reads a numeric value straight into the `UInt32`-backed `IPv4` column (the `TypeIndex::IPv4`
# arm of `insertNumber`), and `Native` casts a source column to the destination type when
# `input_format_native_allow_types_conversion` is enabled (the default), which is valid for a numeric
# column going into `IPv4`. So when a row fails on an unrelated column, an inferred numeric type going
# into an `IPv4` column must NOT be flagged as a structure mismatch for these formats.
#
# In the first two cases below the column `ip` holds a valid numeric value and the column `u` holds a
# string that is not a valid `UUID`, which fails with a genuine parse error (`CANNOT_PARSE_UUID`),
# triggering the diagnostic. An inferred `String` going into a `UUID` destination is compatible (the
# value is re-parsed from the string), so the only candidate for a false positive is the numeric `ip`
# going into the `IPv4` column.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_AVRO=$CLICKHOUSE_TMP/data_04821.avro
DATA_NATIVE=$CLICKHOUSE_TMP/data_04821.native

$CLICKHOUSE_LOCAL -q "SELECT 16909060::Int32 AS ip, 'not-a-uuid' AS u FORMAT Avro" > "$DATA_AVRO"
$CLICKHOUSE_LOCAL -q "SELECT 16909060::UInt32 AS ip, 'not-a-uuid' AS u FORMAT Native" > "$DATA_NATIVE"

echo "-- Avro: a numeric value for an IPv4 column is valid (no false positive)"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT Avro"
    cat "$DATA_AVRO"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- Native: a numeric column is cast to IPv4 under input_format_native_allow_types_conversion (no false positive)"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT Native"
    cat "$DATA_NATIVE"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- Native: a genuine structure mismatch is still explained"
DATA_NATIVE_NESTED=$CLICKHOUSE_TMP/data_04821_nested.native
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'abc' AS x FORMAT Native" > "$DATA_NATIVE_NESTED"
{
    echo "CREATE TABLE t (u UUID, x Array(UInt8)) ENGINE = Memory; INSERT INTO t FORMAT Native"
    cat "$DATA_NATIVE_NESTED"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- Native: with the conversion disabled the type difference is rejected with a conversion error (not a parse error)"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t SETTINGS input_format_native_allow_types_conversion = 0 FORMAT Native"
    cat "$DATA_NATIVE"
} | $CLICKHOUSE_LOCAL 2>&1 | {
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

rm -f "$DATA_AVRO" "$DATA_NATIVE" "$DATA_NATIVE_NESTED"
