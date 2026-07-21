#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the Flatbuffers format requires the flatbuffers/arrow contrib, which is not built in the fast test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# There is no Flatbuffers input format to round-trip through, so we verify the output in two ways:
#  * value-level checks decode the produced FlexBuffers blob and compare the recovered values with
#    the selected ones (this proves the row layout and the type mappings, not just that some
#    non-empty blob was produced), and
#  * observable-property checks confirm that every supported type serializes without crashing, that
#    the root is non-empty and deterministic, and that unsupported types are rejected.

# A minimal, self-contained FlexBuffers decoder for the subset emitted by this format: an untyped
# root vector of untyped row vectors whose elements are Int / UInt / Float / String / Blob / Null /
# (nested) Vector values. It reads the raw output from stdin and prints one decoded row per line.
DECODER=$(cat <<'PYEOF'
import sys, struct

FBT_NULL, FBT_INT, FBT_UINT, FBT_FLOAT = 0, 1, 2, 3
FBT_STRING, FBT_VECTOR, FBT_BLOB = 5, 10, 25

def read_uint(buf, pos, width):
    return int.from_bytes(buf[pos:pos + width], "little", signed=False)

def read_int(buf, pos, width):
    return int.from_bytes(buf[pos:pos + width], "little", signed=True)

def read_float(buf, pos, width):
    return struct.unpack("<f" if width == 4 else "<d", buf[pos:pos + width])[0]

def read_value(buf, pos, parent_width, packed_type):
    typ = packed_type >> 2
    child_width = 1 << (packed_type & 3)
    if typ == FBT_NULL:
        return None
    if typ == FBT_INT:
        return read_int(buf, pos, parent_width)
    if typ == FBT_UINT:
        return read_uint(buf, pos, parent_width)
    if typ == FBT_FLOAT:
        return read_float(buf, pos, parent_width)
    # Offset types: the slot holds an unsigned offset back to the target.
    target = pos - read_uint(buf, pos, parent_width)
    size = read_uint(buf, target - child_width, child_width)
    if typ == FBT_STRING:
        return buf[target:target + size].decode("utf-8")
    if typ == FBT_BLOB:
        return "blob:" + buf[target:target + size].hex()
    if typ == FBT_VECTOR:
        types_at = target + size * child_width
        return [read_value(buf, target + i * child_width, child_width, buf[types_at + i])
                for i in range(size)]
    raise ValueError("unsupported FlexBuffers type %d" % typ)

def get_root(buf):
    byte_width = buf[-1]
    packed_type = buf[-2]
    return read_value(buf, len(buf) - 2 - byte_width, byte_width, packed_type)

def fmt(v):
    if v is None:
        return "null"
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        return v if v.startswith("blob:") else "str:" + v
    if isinstance(v, list):
        return "[" + ", ".join(fmt(x) for x in v) + "]"
    raise ValueError("unexpected value")

for row in get_root(sys.stdin.buffer.read()):
    print(fmt(row))
PYEOF
)

nonempty_output()
{
    local n
    n=$(wc -c)
    [ "$n" -gt 0 ] && echo 1 || echo 0
}

# Value-level check: a single row covering scalars, String, Array, Nullable (NULL) and a
# blob-backed wide integer is decoded back and its values are compared with the selected ones.
$CLICKHOUSE_LOCAL -q "
SELECT
    42::UInt64 AS u,
    -7::Int32 AS i,
    0.1::Float64 AS f,
    'hello'::String AS s,
    [10, 20, 30]::Array(UInt32) AS arr,
    NULL::Nullable(UInt32) AS n,
    123::Int128 AS big
FORMAT Flatbuffers" | python3 -c "$DECODER"

# Value-level check: several rows keep their own per-row values in order.
$CLICKHOUSE_LOCAL -q "SELECT number AS n, toString(number) AS s FROM numbers(3) FORMAT Flatbuffers" | python3 -c "$DECODER"

# Value-level check: FixedString and UUID map to String, and wide integers are serialized as
# little-endian Blobs. 256 makes the byte order observable (0x00 0x01 ... in little-endian).
$CLICKHOUSE_LOCAL -q "
SELECT
    'abcd'::FixedString(4) AS fs,
    toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid,
    256::Int128 AS le
FORMAT Flatbuffers" | python3 -c "$DECODER"

# Value-level check for the remaining non-trivial mapping branches. A single row lets the decoded
# values be compared directly, proving each mapping (not just that some non-empty blob is produced):
#  * Date/Date32 map to the day number, DateTime to the Unix timestamp and DateTime64 to the raw
#    (scaled) count, so the decoded integers pin the temporal encoding;
#  * Decimal64 maps to its raw (unscaled) integer (1.5 with scale 3 -> 1500);
#  * IPv4 maps to the numeric address as a UInt, IPv6 to its 16-byte network-order Blob (so ::1
#    decodes to the trailing 0x01);
#  * Tuple maps to a nested vector, LowCardinality to the underlying value (the dictionary value
#    'lc', not its index), and FixedString/UUID to their String form.
$CLICKHOUSE_LOCAL -q "
SELECT
    42::UInt64 AS u64,
    -5::Int32 AS i32,
    0.5::Float32 AS f32,
    0.25::Float64 AS f64,
    'abcd'::FixedString(4) AS fs,
    toDate('2020-01-01') AS d,
    toDate32('2020-01-01') AS d32,
    toDateTime('2020-01-01 00:00:00', 'UTC') AS dt,
    toDateTime64('2020-01-01 00:00:00.123', 3, 'UTC') AS dt64,
    toDecimal64('1.5', 3) AS dec64,
    toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid,
    toIPv4('1.2.3.4') AS ipv4,
    toIPv6('::1') AS ipv6,
    [10, 20]::Array(UInt32) AS arr,
    (7, 'k')::Tuple(UInt32, String) AS tup,
    NULL::Nullable(UInt32) AS nullable,
    toLowCardinality('lc') AS lc
FORMAT Flatbuffers" | python3 -c "$DECODER"

# Value-level check: wide integers and large decimals are serialized as little-endian Blobs, and
# enums map to their underlying Int. 42 -> 0x2a as the first (lowest) byte; 42.42 with scale 2 has
# the raw value 4242 = 0x1092, so the Blob starts with 0x92 0x10 in little-endian order.
$CLICKHOUSE_LOCAL -q "
SELECT
    42::Int128 AS i128, 42::UInt128 AS u128, 42::Int256 AS i256, 42::UInt256 AS u256,
    '42.42'::Decimal128(2) AS dec128, '42.42'::Decimal256(2) AS dec256,
    'a'::Enum8('a' = 1) AS e8, 'b'::Enum16('b' = 2) AS e16
FORMAT Flatbuffers" | python3 -c "$DECODER"

# An empty result set still produces a valid (non-empty) FlexBuffers root that decodes to zero rows.
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x WHERE 0 FORMAT Flatbuffers" | nonempty_output
echo "rows=$($CLICKHOUSE_LOCAL -q "SELECT 1 AS x WHERE 0 FORMAT Flatbuffers" | python3 -c "$DECODER" | wc -l)"

# Serialization is deterministic: the same query yields byte-identical output.
out1=$($CLICKHOUSE_LOCAL -q "SELECT number, toString(number) FROM numbers(5) FORMAT Flatbuffers" | md5sum)
out2=$($CLICKHOUSE_LOCAL -q "SELECT number, toString(number) FROM numbers(5) FORMAT Flatbuffers" | md5sum)
[ "$out1" = "$out2" ] && echo 1 || echo 0

# Unsupported types are rejected with a clear error.
$CLICKHOUSE_LOCAL -q "SELECT map('k', 1) AS m FORMAT Flatbuffers" 2>&1 >/dev/null | grep -o -F "is not supported for Flatbuffers output format" | head -n 1
