#!/usr/bin/env bash
# Tags: no-ordinary-database, zookeeper, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

table_name="02704_keeper_map_zk_nodes"
table_name_another="02704_keeper_map_zk_nodes_new_table"

$CLICKHOUSE_CLIENT --query="
DROP TABLE IF EXISTS $table_name;
DROP TABLE IF EXISTS $table_name_another;
CREATE TABLE $table_name (key UInt64, value UInt64)
ENGINE = KeeperMap('/$table_name/$CLICKHOUSE_DATABASE')
PRIMARY KEY(key)"

function assert_children_size()
{
    for _ in `seq 10`
    do
        children_size=$($CLICKHOUSE_CLIENT --query="SELECT count() FROM system.zookeeper WHERE path = '$1'")
        if [ $children_size == $2 ]
        then
            return
        fi

        sleep 0.4
    done

    echo "Invalid number of children for path '$1': actual $children_size, expected $2"
    exit 1
}

function assert_root_children_size()
{
    assert_children_size "/test_keeper_map/02704_keeper_map_zk_nodes/$CLICKHOUSE_DATABASE" $1
}

function assert_data_children_size()
{
    assert_children_size "/test_keeper_map/02704_keeper_map_zk_nodes/$CLICKHOUSE_DATABASE/data" $1
}

assert_root_children_size 2
assert_data_children_size 0

$CLICKHOUSE_CLIENT --query="INSERT INTO $table_name VALUES (1, 11)"

assert_data_children_size 1

$CLICKHOUSE_CLIENT --query="
CREATE TABLE $table_name_another (key UInt64, value UInt64)
ENGINE = KeeperMap('/$table_name/$CLICKHOUSE_DATABASE')
PRIMARY KEY(key)"

assert_root_children_size 2
assert_data_children_size 1

$CLICKHOUSE_CLIENT --query="INSERT INTO $table_name_another VALUES (1, 11)"

assert_root_children_size 2
assert_data_children_size 1

$CLICKHOUSE_CLIENT --query="INSERT INTO $table_name_another VALUES (2, 22)"

assert_root_children_size 2
assert_data_children_size 2

$CLICKHOUSE_CLIENT --query="DROP TABLE $table_name"

assert_root_children_size 2
assert_data_children_size 2

$CLICKHOUSE_CLIENT --query="DROP TABLE $table_name_another"

assert_root_children_size 0
