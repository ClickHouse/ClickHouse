#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the Flatbuffers format requires the flatbuffers/arrow contrib, which is not built in the fast test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The Flatbuffers output is an opaque, schema-less FlexBuffers blob and there is no Flatbuffers input
# format to round-trip through, so we assert observable properties instead of comparing golden bytes:
#  * every supported type is serialized without crashing (the matrix below exercises each branch),
#  * the produced FlexBuffers root is non-empty,
#  * serialization is deterministic, and
#  * unsupported types are rejected with a clear error.

nonempty_output()
{
    local n
    n=$(wc -c)
    [ "$n" -gt 0 ] && echo 1 || echo 0
}

# Numeric / string / temporal / container / nullable / low-cardinality types.
$CLICKHOUSE_LOCAL -q "
SELECT
    number AS u64,
    toInt32(number) - 5 AS i32,
    toFloat32(number) / 2 AS f32,
    toFloat64(number) / 3 AS f64,
    toString(number) AS s,
    toFixedString(toString(number), 4) AS fs,
    toDate('2020-01-01') + number AS d,
    toDate32('2020-01-01') + number AS d32,
    toDateTime('2020-01-01 00:00:00', 'UTC') + number AS dt,
    toDateTime64('2020-01-01 00:00:00.123', 3, 'UTC') AS dt64,
    toDecimal64(number, 3) AS dec64,
    toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid,
    toIPv4('1.2.3.4') AS ipv4,
    toIPv6('::1') AS ipv6,
    [number, number + 1] AS arr,
    (number, toString(number)) AS tup,
    if(number % 2 = 0, NULL, number)::Nullable(UInt32) AS nullable,
    toLowCardinality(toString(number)) AS lc
FROM numbers(3)
FORMAT Flatbuffers" | nonempty_output

# Wide integers, large decimals (serialized as Blob), enums and Float32.
$CLICKHOUSE_LOCAL -q "
SELECT
    42::Int128 AS i128, 42::UInt128 AS u128, 42::Int256 AS i256, 42::UInt256 AS u256,
    42.42::Decimal128(2) AS dec128, 42.42::Decimal256(2) AS dec256,
    'a'::Enum8('a' = 1) AS e8, 'b'::Enum16('b' = 1) AS e16
FORMAT Flatbuffers" | nonempty_output

# An empty result set still produces a valid (non-empty) FlexBuffers root.
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x WHERE 0 FORMAT Flatbuffers" | nonempty_output

# Serialization is deterministic: the same query yields byte-identical output.
out1=$($CLICKHOUSE_LOCAL -q "SELECT number, toString(number) FROM numbers(5) FORMAT Flatbuffers" | md5sum)
out2=$($CLICKHOUSE_LOCAL -q "SELECT number, toString(number) FROM numbers(5) FORMAT Flatbuffers" | md5sum)
[ "$out1" = "$out2" ] && echo 1 || echo 0

# Unsupported types are rejected with a clear error.
$CLICKHOUSE_LOCAL -q "SELECT map('k', 1) AS m FORMAT Flatbuffers" 2>&1 >/dev/null | grep -o -F "is not supported for Flatbuffers output format" | head -n 1
