#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet`, `Arrow` and `ORC` are not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/pull/110626
# Regression tests for typed JSON token-shape mismatches. These settings change the parser's accepted
# token shape, so the diagnostic must not let the generic inferred-type compatibility hide them.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow, object is invalid for Map when arrays of tuples are enabled"
printf 'CREATE TABLE t (m Map(String, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"m":{"k":1},"n":1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_map_as_array_of_tuples 1 2>&1 | check

echo "-- JSONEachRow, malformed array is invalid for Map when arrays of tuples are enabled"
printf 'CREATE TABLE t (m Map(String, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"m":[1,2],"n":1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_map_as_array_of_tuples 1 2>&1 | check

echo "-- JSONEachRow, homogeneous array with the wrong arity is invalid for Tuple"
printf 'CREATE TABLE t (t Tuple(UInt8, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"t":[1],"n":1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, object is invalid for a named Tuple when object reading is disabled"
printf 'CREATE TABLE t (x Tuple(a UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"x":{"a":1},"n":1.5}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_named_tuples_as_objects 0 2>&1 | check

echo "-- JSONEachRow, scalar is invalid for JSON"
printf 'CREATE TABLE t (j JSON, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"j":1,"n":1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONCompactColumns, an extra positional column is a mismatch"
printf 'CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONCompactColumns [[1],[2],["x"]]\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

# The destination is a nested type: casting the decoded `String` source column into it fails with
# `CANNOT_READ_ARRAY_FROM_TEXT`, a genuine parse error, so the diagnostic runs. A `String` source going
# into a numeric column cannot be used here: that cast fails with `CANNOT_PARSE_TEXT`, which is not
# classified as a parse error, so no diagnostic is attached to it at all.
for format in Parquet Arrow ORC; do
    data_file="$CLICKHOUSE_TMP/data_04903_${format}"
    setting="input_format_${format,,}_case_insensitive_column_matching"
    $CLICKHOUSE_LOCAL -q "SELECT 'not_an_array' AS A FORMAT $format" > "$data_file"

    echo "-- $format, case-insensitive source name still attributes a type mismatch"
    {
        echo "CREATE TABLE t (a Array(UInt8)) ENGINE = Memory; INSERT INTO t SETTINGS $setting = 1 FORMAT $format"
        cat "$data_file"
    } | $CLICKHOUSE_LOCAL 2>&1 | check

    rm -f "$data_file"
done
