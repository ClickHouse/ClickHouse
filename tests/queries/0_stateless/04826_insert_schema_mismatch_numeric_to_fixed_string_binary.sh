#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `MsgPack` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A numeric value into a `FixedString` column is a genuine structure mismatch in the binary formats
# that store typed values: `MsgPack` routes a `FixedString` column only through its string / binary
# insertion path (the integer path has no `FixedString` arm), and `BSONEachRow` reads a `FixedString`
# column only from the string / binary BSON tags, rejecting the numeric ones. So the schema-mismatch
# diagnostic must report it there, while a string value for the same column stays compatible.
#
# In every document below the field `u` is a binary value of 4 bytes for a `UUID` column, which fails
# with a genuine parse error (a `UUID` requires 16 bytes), triggering the diagnostic. It is placed
# first so the parse error fires before the `f` field is read. The cases differ only in the kind of
# value stored for the `FixedString` column `f`.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- BSONEachRow: an integer value for a FixedString column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, f FixedString(3)) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), f: int32 1}
    printf '\x18\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x10f\x00\x01\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: a string value for a FixedString column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, f FixedString(3)) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), f: string 'abc'}
    printf '\x1c\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x02f\x00\x04\x00\x00\x00abc\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- MsgPack: an integer value for a FixedString column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, f FixedString(3)) ENGINE = Memory; INSERT INTO t SETTINGS input_format_msgpack_number_of_columns = 2 FORMAT MsgPack"
    # bin8(4 bytes 'AAAA'), positive fixint 1
    printf '\xc4\x04AAAA\x01'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- MsgPack: a string value for a FixedString column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, f FixedString(3)) ENGINE = Memory; INSERT INTO t SETTINGS input_format_msgpack_number_of_columns = 2 FORMAT MsgPack"
    # bin8(4 bytes 'AAAA'), fixstr 'abc'
    printf '\xc4\x04AAAA\xa3abc'
} | $CLICKHOUSE_LOCAL 2>&1 | check
