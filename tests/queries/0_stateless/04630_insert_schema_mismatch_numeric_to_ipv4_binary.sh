#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The text / JSON deserializers require a (quoted) string for an `IPv4` column and reject a bare
# number, so an inferred numeric type going into an `IPv4` column is a genuine structure mismatch
# there. The binary formats that store typed values are different: `BSONEachRow` reads a BSON `Int32`
# and `MsgPack` reads an integer straight into the `UInt32`-backed `IPv4` column, so a numeric value
# is valid and must NOT be flagged as a structure mismatch for them. `UUID` and `IPv6` still require
# binary data of the exact size in those formats, so a numeric value there stays a mismatch.
#
# In both documents below the field `u` is a BSON binary with the UUID subtype but only 4 bytes of
# payload, which fails with a genuine parse error (`INCORRECT_DATA`, wrong binary size for a UUID),
# triggering the diagnostic. It is placed first so the parse error fires before the numeric `ip`
# field is read. The field `ip` is a BSON int32. The two cases differ only in the type of the `ip`
# column, so they exercise exactly the fix (`IPv4` accepts the numeric value) and its boundary
# (`IPv6` still requires binary data of the exact size, so a numeric value stays a mismatch).

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- BSONEachRow: an int32 value for an IPv4 column is valid (no false positive)"
{
    echo "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), ip: int32 1}
    printf '\x19\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x10ip\x00\x01\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: an int32 value for an IPv6 column stays a genuine structure mismatch"
{
    echo "CREATE TABLE t (ip IPv6, u UUID) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), ip: int32 1}
    printf '\x19\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x10ip\x00\x01\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check
