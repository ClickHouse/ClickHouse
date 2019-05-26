#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

function create {
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS summing"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS collapsing"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS aggregating"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS replacing"

    $CLICKHOUSE_CLIENT --query="CREATE TABLE summing (d Date DEFAULT today(), x UInt64, s UInt64 DEFAULT 1) ENGINE = SummingMergeTree(d, x, 8192)"
    $CLICKHOUSE_CLIENT --query="CREATE TABLE collapsing (d Date DEFAULT today(), x UInt64, s Int8 DEFAULT 1) ENGINE = CollapsingMergeTree(d, x, 8192, s)"
    $CLICKHOUSE_CLIENT --query="CREATE TABLE aggregating (d Date DEFAULT today(), x UInt64, s AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree(d, x, 8192)"
    $CLICKHOUSE_CLIENT --query="CREATE TABLE replacing (d Date DEFAULT today(), x UInt64, s Int8 DEFAULT 1, v UInt64) ENGINE = ReplacingMergeTree(d, (x), 8192, v)"
}


function cleanup {
    $CLICKHOUSE_CLIENT --query="DROP TABLE summing"
    $CLICKHOUSE_CLIENT --query="DROP TABLE collapsing"
    $CLICKHOUSE_CLIENT --query="DROP TABLE aggregating"
    $CLICKHOUSE_CLIENT --query="DROP TABLE replacing"
}


function test {
    create

    SUM=$(( $1 + $2 ))
    MAX=$(( $1 > $2 ? $1 : $2 ))

    SETTINGS="--min_insert_block_size_rows=0 --min_insert_block_size_bytes=0"

    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO summing (x) SELECT number AS x FROM system.numbers LIMIT $1"
    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO summing (x) SELECT number AS x FROM system.numbers LIMIT $2"

    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO collapsing (x) SELECT number AS x FROM system.numbers LIMIT $1"
    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO collapsing (x) SELECT number AS x FROM system.numbers LIMIT $2"

    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO aggregating (d, x, s) SELECT today() AS d, number AS x, sumState(materialize(toUInt64(1))) AS s FROM (SELECT number FROM system.numbers LIMIT $1) GROUP BY number"
    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO aggregating (d, x, s) SELECT today() AS d, number AS x, sumState(materialize(toUInt64(1))) AS s FROM (SELECT number FROM system.numbers LIMIT $2) GROUP BY number"

    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO replacing (x, v) SELECT number AS x, toUInt64(number % 3 == 0) FROM system.numbers LIMIT $1"
    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO replacing (x, v) SELECT number AS x, toUInt64(number % 3 == 1) FROM system.numbers LIMIT $2"

    $CLICKHOUSE_CLIENT --query="SELECT count() = $SUM, sum(s) = $SUM FROM summing"
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE summing"
    $CLICKHOUSE_CLIENT --query="SELECT count() = $MAX, sum(s) = $SUM FROM summing"
    echo
    $CLICKHOUSE_CLIENT --query="SELECT count() = $SUM, sum(s) = $SUM FROM collapsing"
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE collapsing" --server_logs_file='/dev/null';
    $CLICKHOUSE_CLIENT --query="SELECT count() = $MAX, sum(s) = $MAX FROM collapsing"
    echo
    $CLICKHOUSE_CLIENT --query="SELECT count() = $SUM, sumMerge(s) = $SUM FROM aggregating"
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE aggregating"
    $CLICKHOUSE_CLIENT --query="SELECT count() = $MAX, sumMerge(s) = $SUM FROM aggregating"
    echo
    $CLICKHOUSE_CLIENT --query="SELECT count() = $SUM, sum(s) = $SUM FROM replacing"
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE replacing"
    $CLICKHOUSE_CLIENT --query="SELECT count() = $MAX, sum(s) = $MAX FROM replacing"
    $CLICKHOUSE_CLIENT --query="SELECT count() = sum(v) FROM replacing where x % 3 == 0 and x < $1"
    $CLICKHOUSE_CLIENT --query="SELECT count() = sum(v) FROM replacing where x % 3 == 1 and x < $2"
    $CLICKHOUSE_CLIENT --query="SELECT sum(v) = 0 FROM replacing where x % 3 == 2"
    echo
    echo
}

merged_rows_0=`$CLICKHOUSE_CLIENT -q "select value from system.events where event = 'MergedRows'"`

test 8191 8191
test 8191 8192
test 8192 8191
test 8192 8192
test 8192 8193
test 8193 8192
test 8193 8193
test 8191 8193
test 8193 8191
test 8193 8194
test 8194 8193
test 8194 8194

merged_rows_1=`$CLICKHOUSE_CLIENT -q "select value from system.events where event = 'MergedRows'"`
[[ $merged_rows_1 -le $merged_rows_0 ]]

cleanup
