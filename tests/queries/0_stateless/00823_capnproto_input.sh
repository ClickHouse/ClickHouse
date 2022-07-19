#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

#create the schema file
echo "
@0x803231eaa402b968;
struct NestedNestedOne
{
    nestednestednumber @0 : UInt64;
}
struct NestedNestedTwo
{
    nestednestedtext @0 : Text;
}
struct NestedOne
{
    nestednestedone @0 : NestedNestedOne;
    nestednestedtwo @1 : NestedNestedTwo;
    nestednumber @2: UInt64;
}
struct NestedTwo
{
    nestednestedone @0 : NestedNestedOne;
    nestednestedtwo @1 : NestedNestedTwo;
    nestedtext @2 : Text;
}
struct CapnProto
{
    number @0 : UInt64;
    string @1 : Text;
    nestedone @2 : NestedOne;
    nestedtwo @3 : NestedTwo;
    nestedthree @4 : NestedNestedTwo;
}" > "${CLICKHOUSE_TMP}"/test.capnp

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS capnproto_input"
$CLICKHOUSE_CLIENT -q "CREATE TABLE capnproto_input
(
    number UInt64,
    string String,
    nestedone_nestednumber UInt64,
    nestedone_nestednestedone_nestednestednumber UInt64,
    nestedone_nestednestedtwo_nestednestedtext String,
    nestedtwo_nestednestedtwo_nestednestedtext String,
    nestedtwo_nestednestedone_nestednestednumber UInt64,
    nestedtwo_nestedtext String
) ENGINE = Memory"

echo  -ne '\x00\x00\x00\x00\x15\x00\x00\x00\x00\x00\x00\x00\x01\x00\x04\x00\x01\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x12\x00\x00\x00\x0c\x00\x00\x00\x01\x00\x02\x00\x20\x00\x00\x00\x00\x00\x03\x00\x34\x00\x00\x00\x00\x00\x01\x00\x32\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x00\x00\x01\x00\x03\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x12\x00\x00\x00\x34\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x01\x00\x00\x00\x08\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x12\x00\x00\x00\x37\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x12\x00\x00\x00\x39\x00\x00\x00\x00\x00\x00\x00' | $CLICKHOUSE_CLIENT --stacktrace --format_schema="${CLICKHOUSE_TMP}/test:CapnProto" --query="INSERT INTO capnproto_input FORMAT CapnProto";

$CLICKHOUSE_CLIENT -q "SELECT * FROM capnproto_input"
$CLICKHOUSE_CLIENT -q "DROP TABLE capnproto_input"

# remove the schema file
rm "${CLICKHOUSE_TMP}"/test.capnp
