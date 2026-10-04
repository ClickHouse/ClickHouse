#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `MsgPack` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The binary formats that store typed values (`BSONEachRow`, `MsgPack`) keep the on-wire numeric
# kind, and their parsers do not convert it across the integer / floating-point family boundary:
# a stored double is accepted only into a `Float*` column, and a stored integer is rejected for a
# `Float*` column in turn — unlike the text / JSON formats, where any numeric token is re-parsed
# by the destination's deserializer. So there an inferred floating-point type is a genuine
# structure mismatch for a non-floating-point destination (and vice versa) and must be reported.
#
# In every document below the field `u` is a binary value of 4 bytes for a `UUID` column, which
# fails with a genuine parse error (a `UUID` requires 16 bytes), triggering the diagnostic. It is
# placed first so the parse error fires before the numeric `x` field is read. The cases differ
# only in the on-wire kind of `x` and the type of the `x` column.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- BSONEachRow: a double value for a UInt8 column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, x UInt8) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: double 1.5}
    printf '\x1c\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x01x\x00\x00\x00\x00\x00\x00\x00\xf8\x3f\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: a double value for a Float32 column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, x Float32) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: double 1.5}
    printf '\x1c\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x01x\x00\x00\x00\x00\x00\x00\x00\xf8\x3f\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: an int32 value for a Float64 column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, x Float64) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: int32 1}
    printf '\x18\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x10x\x00\x01\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: an int32 value for a UInt8 column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, x UInt8) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: int32 1}
    printf '\x18\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x10x\x00\x01\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- MsgPack: a float64 value for a UInt8 column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, x UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_msgpack_number_of_columns = 2 FORMAT MsgPack"
    # bin8(4 bytes 'AAAA'), float64 1.5
    printf '\xc4\x04AAAA\xcb\x3f\xf8\x00\x00\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- MsgPack: a float64 value for a Float64 column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, x Float64) ENGINE = Memory; INSERT INTO t SETTINGS input_format_msgpack_number_of_columns = 2 FORMAT MsgPack"
    # bin8(4 bytes 'AAAA'), float64 1.5
    printf '\xc4\x04AAAA\xcb\x3f\xf8\x00\x00\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- MsgPack: an integer value for a Float64 column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, x Float64) ENGINE = Memory; INSERT INTO t SETTINGS input_format_msgpack_number_of_columns = 2 FORMAT MsgPack"
    # bin8(4 bytes 'AAAA'), positive fixint 1
    printf '\xc4\x04AAAA\x01'
} | $CLICKHOUSE_LOCAL 2>&1 | check
