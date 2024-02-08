#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --allow_experimental_object_type=1"

function create_table()
{
    $CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, Float64, Bool, String, DateTime, UUID,
                                Array(UInt64), Map(String, UInt64), Tuple(i UInt8, s String), IPv4, Ring)) engine=$1;"
}

function drop_table()
{
    $CH_CLIENT -q "drop table if exists test;"
}

function insert_data()
{
  $CH_CLIENT -q  "insert into test select number, number from numbers(3);"
}

function update_data()
{
    data_type="UInt64"
    $CH_CLIENT -nmq "select id, v.$data_type from test;"

    # Update Variant data while keeping the existing data type
    $CH_CLIENT -q  "alter table test update v = 1::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.$data_type from test;"
    # -----------------------------------------------------------------

    # Update Variant data while changing to a different data type
    data_type="Float64"
    $CH_CLIENT -q  "alter table test update v = (1 - 0.9)::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="Bool"
    $CH_CLIENT -q  "alter table test update v = true::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="DateTime"
    $CH_CLIENT -q  "alter table test update v = '2019-01-01 00:00:00'::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="UUID"
    $CH_CLIENT -q  "alter table test update v = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="Array(UInt64)"
    $CH_CLIENT -q  "alter table test update v = [1,2,3,4,5]::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="Map(String, UInt64)"
    $CH_CLIENT -q  "alter table test update v = map('one', 1, 'two', 2, 'three', 3)::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="Tuple(i UInt8, s String)"
    $CH_CLIENT -q  "alter table test update v = (1, 'one')::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="IPv4"
    $CH_CLIENT -q  "alter table test update v = '116.253.40.133'::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="Ring"
    $CH_CLIENT -q  "alter table test update v = [(0, 0), (10, 0), (10, 10), (0, 10)]::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"

    data_type="String"
    $CH_CLIENT -q  "alter table test update v = 'Hello'::$data_type where 1;"
    $CH_CLIENT -nmq "select id, v.\`$data_type\` from test order by id;"
    # -----------------------------------------------------------------
}

function delete_data()
{
    echo "Delete Data"

    # Deleting with a condition on non-Variant column
    $CH_CLIENT -q  "alter table test delete where id = 2;"
    $CH_CLIENT -nmq "select id, v from test order by id;"

    # Delete with a condition on Variant column value
    #$CH_CLIENT -q  "alter table test delete where v = 'Hello';"
    #$CH_CLIENT -nmq "select id, v from test order by id;"
}

engines=("MergeTree order by id settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000" "MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1")

for engine in "${engines[@]}"; do

   echo "$engine"

   drop_table

   create_table "$engine"

   insert_data

   update_data

   delete_data

done




