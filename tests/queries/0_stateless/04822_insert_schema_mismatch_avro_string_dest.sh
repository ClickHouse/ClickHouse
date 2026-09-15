#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Avro` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Avro` accepts only the string-family wire types (`string`, `bytes`, `fixed`, `enum`) into a
# `String` destination column and rejects the numeric ones — but that rejection happens eagerly,
# when the deserializer is built from the schema in the file header (`createDeserializeFn` throws
# `ILLEGAL_COLUMN` "Type String is not compatible with Avro int" while reading the header), before
# any value is decoded. So a value-level parse error on another column can never coexist with such
# a mismatch, and the diagnostic (which only fires on parse errors) never needs to report it: the
# parser's own error already names the column and both types. This pins that division of labor.

PHRASE="does not match the structure expected by the query"

DATA_STR=$CLICKHOUSE_TMP/data_04822_str.avro
DATA_INT=$CLICKHOUSE_TMP/data_04822_int.avro
DATA_ENUM=$CLICKHOUSE_TMP/data_04822_enum.avro

$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'hello' AS s FORMAT Avro" > "$DATA_STR"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 7::Int32 AS s FORMAT Avro" > "$DATA_INT"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, CAST('a', 'Enum8(''a'' = 1)') AS s FORMAT Avro" > "$DATA_ENUM"

echo "-- Avro string into String: a parse error on the other column gets no false-positive suffix"
{
    echo "CREATE TABLE t (u UUID, s String) ENGINE = Memory; INSERT INTO t FORMAT Avro"
    cat "$DATA_STR"
} | $CLICKHOUSE_LOCAL 2>&1 | {
    out=$(cat)
    if echo "$out" | grep -q "CANNOT_PARSE_UUID"; then echo "parse error as expected"; else echo "unexpected error"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- Avro enum into String: accepted by the parser, no false-positive suffix"
{
    echo "CREATE TABLE t (u UUID, s String) ENGINE = Memory; INSERT INTO t FORMAT Avro"
    cat "$DATA_ENUM"
} | $CLICKHOUSE_LOCAL 2>&1 | {
    out=$(cat)
    if echo "$out" | grep -q "CANNOT_PARSE_UUID"; then echo "parse error as expected"; else echo "unexpected error"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- Avro int into String: rejected eagerly while reading the header, before any value is parsed"
{
    echo "CREATE TABLE t (u UUID, s String) ENGINE = Memory; INSERT INTO t FORMAT Avro"
    cat "$DATA_INT"
} | $CLICKHOUSE_LOCAL 2>&1 | {
    out=$(cat)
    if echo "$out" | grep -q "is not compatible with Avro"; then echo "incompatibility reported by the parser"; else echo "unexpected error"; fi
    if echo "$out" | grep -q "CANNOT_PARSE_UUID"; then echo "unexpected parse error"; else echo "no parse error"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

rm -f "$DATA_STR" "$DATA_INT" "$DATA_ENUM"
