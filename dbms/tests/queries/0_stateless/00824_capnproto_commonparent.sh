#!/usr/bin/env bash

set -e 

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

#create the schema file
echo "
@0xaba30bcbb11d41b6;

struct NestedOne {
    parent @0 :NestedTwo;
}

struct NestedTwo {
    child @0 :Text;
}

struct CommonParent {
    nestedone   @0 : NestedOne;
    nestedtwo   @1 : NestedOne;
}
" > test.capnp

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.capnproto_parent"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test.capnproto_parent
(
    nestedone_parent_child String,
    nestedtwo_parent_child String
) ENGINE = Memory"

echo -ne '\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x04\x00\x00\x00\x00\x00\x01\x00\x0c\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00\x22\x00\x00\x00\x6f\x6e\x65\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00\x22\x00\x00\x00\x74\x77\x6f\x00\x00\x00\x00\x00' | $CLICKHOUSE_CLIENT --stacktrace --format_schema='test:CommonParent' --query="INSERT INTO test.capnproto_parent FORMAT CapnProto";

$CLICKHOUSE_CLIENT -q "SELECT * FROM test.capnproto_parent"
$CLICKHOUSE_CLIENT -q "DROP TABLE test.capnproto_parent"

# remove the schema file
rm test.capnp
