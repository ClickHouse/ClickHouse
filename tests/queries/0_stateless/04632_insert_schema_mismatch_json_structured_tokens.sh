#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# Schema inference canonicalizes structured JSON values differently from the parser: a homogeneous
# `[...]` becomes `Array(...)` (`transformTuplesWithEqualNestedTypesToArrays`) and a `{...}` becomes a
# named `Tuple` (`input_format_json_try_infer_named_tuples_from_objects`, enabled by default) — while
# the parser still reads the same `[...]` token into an unnamed `Tuple` and the same `{...}` token into
# a `Map` or a named `Tuple`. The schema-mismatch diagnostic must follow the parser: valid structured
# columns must not pick up a misleading explanation when an unrelated column has a parse error, and a
# genuinely wrong structured token must still be explained.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow, array into unnamed Tuple and object into Map, unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (t Tuple(UInt8, UInt8), m Map(String, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"t": [1, 2], "m": {"k": 1}, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, heterogeneous array (inferred as Array(Dynamic)) into unnamed Tuple, unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (t Tuple(UInt8, String), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"t": [1, "a"], "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, object into named Tuple with a subset of the keys, unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (t Tuple(a UInt8, b String), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"t": {"a": 1}, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, object with a numeric-string key into Map(UInt64, ...), unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (m Map(UInt64, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"m": {"1": 1}, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, string elements into a Tuple of UUIDs (parsed from text), unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (t Tuple(UUID, UUID), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"t": ["2fbef46e-2fe4-4d05-bb73-b78ad9071ad4", "2fbef46e-2fe4-4d05-bb73-b78ad9071ad5"], "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, array of scalars into a Tuple of Arrays (genuine mismatch)"
printf 'CREATE TABLE t (t Tuple(Array(UInt8), Array(UInt8))) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"t": [1, 2]}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, object with an unknown key into a named Tuple with input_format_json_ignore_unknown_keys_in_named_tuple = 0 (genuine mismatch)"
printf 'CREATE TABLE t (t Tuple(a UInt8)) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"t": {"a": 1, "z": 2}}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_ignore_unknown_keys_in_named_tuple 0 2>&1 | check

echo "-- JSONEachRow, array token into a Map (genuine mismatch)"
printf 'CREATE TABLE t (m Map(String, UInt8)) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"m": [1, 2]}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
