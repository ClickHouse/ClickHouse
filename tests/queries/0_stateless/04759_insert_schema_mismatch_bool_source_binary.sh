#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `MsgPack` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A boolean value in the binary formats that store typed values behaves exactly like a stored
# integer: `BSONEachRow` accepts a BSON `Bool` into any integer-backed column (`UInt8`, `Enum*`,
# `Date*`, `DateTime`, ...) and rejects it for a `Float*` column, and `MsgPack` routes a boolean
# through the same insertion path as an integer. Schema inference reports a BSON boolean field as
# `Bool` — which is a `UInt8` with a custom name, so the compatibility rules must treat it exactly
# like an inferred integer: no "structure mismatch" explanation when a valid boolean field
# accompanies an unrelated parse error, and a genuine mismatch reported for a `Float*` destination.
#
# In every document below the field `u` is a binary value of 4 bytes for a `UUID` column, which
# fails with a genuine parse error (a `UUID` requires 16 bytes), triggering the diagnostic. It is
# placed first so the parse error fires before the boolean `x` field is read. The cases differ
# only in the type of the `x` column.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- BSONEachRow: a boolean value for a Bool column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, x Bool) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: bool true}
    printf '\x15\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x08x\x00\x01\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: a boolean value for a UInt8 column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, x UInt8) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: bool true}
    printf '\x15\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x08x\x00\x01\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: a boolean value for a DateTime column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, x DateTime) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: bool true}
    printf '\x15\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x08x\x00\x01\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: a boolean value for a Float64 column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, x Float64) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: bool true}
    printf '\x15\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x08x\x00\x01\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

for type in 'Decimal32(2)' DateTime64 IPv4; do
    echo "-- BSONEachRow: a boolean value for a $type column is a genuine structure mismatch"
    {
        echo "CREATE TABLE t (u UUID, x $type) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
        # {u: Binary(subtype UUID, 4 bytes 'AAAA'), x: bool true}
        printf '\x15\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x08x\x00\x01\x00'
    } | $CLICKHOUSE_LOCAL 2>&1 | check
done

echo "-- MsgPack: a boolean value for a UInt8 column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, x UInt8) ENGINE = Memory; INSERT INTO t SETTINGS input_format_msgpack_number_of_columns = 2 FORMAT MsgPack"
    # bin8(4 bytes 'AAAA'), bool true
    printf '\xc4\x04AAAA\xc3'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- MsgPack: a boolean value for a Float64 column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (u UUID, x Float64) ENGINE = Memory; INSERT INTO t SETTINGS input_format_msgpack_number_of_columns = 2 FORMAT MsgPack"
    # bin8(4 bytes 'AAAA'), bool true
    printf '\xc4\x04AAAA\xc3'
} | $CLICKHOUSE_LOCAL 2>&1 | check
