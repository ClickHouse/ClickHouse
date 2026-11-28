#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `BSONEachRow` reads only a BSON `Int32` into an `IPv4` column: `readAndInsertIPv4` rejects every
# other element type, including `Int64` and `Double`. So an `Int64` or `Double` value for an `IPv4`
# column is a genuine structure mismatch and must be explained, while an `Int32` value is valid.
#
# In every document below the field `u` is a BSON binary with the UUID subtype but only 4 bytes of
# payload, which fails with a genuine parse error (`INCORRECT_DATA`, wrong binary size for a UUID)
# before the `ip` field is read, triggering the diagnostic. The documents differ only in the element
# type of the `ip` field.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- int32 into IPv4: valid (no false positive)"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), ip: int32 1}
    printf '\x19\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x10ip\x00\x01\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- int64 into IPv4: a genuine structure mismatch"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), ip: int64 1}
    printf '\x1d\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x12ip\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- double into IPv4: a genuine structure mismatch"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), ip: double 1.0}
    printf '\x1d\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x01ip\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check
