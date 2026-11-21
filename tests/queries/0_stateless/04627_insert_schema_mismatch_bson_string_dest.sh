#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `BSONEachRow` stores typed values, and the parser rejects a non-string value for a `String`
# destination column ("Cannot insert BSON int32 into String column"), unlike the flat-text formats,
# which read every field verbatim. The schema-mismatch explanation must treat an inferred non-String
# type going into a `String` column as a genuine mismatch for such formats.
#
# In both documents below the field `u` is a BSON binary with the UUID subtype but only 4 bytes of
# payload, which fails with a genuine parse error (`INCORRECT_DATA`, wrong binary size for a UUID),
# triggering the diagnostic. The field `s` is an int32 in the first document (a real structure
# mismatch for the `String` column) and a string in the second (no mismatch).

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- BSONEachRow: an int32 value for a String column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, s String) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), s: int32 1}
    printf '\x18\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x10s\x00\x01\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: a string value for a String column, only another field fails (no false positive)"
{
    echo "CREATE TABLE t (u UUID, s String) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), s: "hi"}
    printf '\x1b\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x02s\x00\x03\x00\x00\x00hi\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check
