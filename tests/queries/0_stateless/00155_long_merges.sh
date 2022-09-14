#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function create {
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS summing_00155"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS collapsing_00155"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS aggregating_00155"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS replacing_00155"

    $CLICKHOUSE_CLIENT --query="CREATE TABLE summing_00155 (d Date DEFAULT today(), x UInt64, s UInt64 DEFAULT 1) ENGINE = SummingMergeTree(d, x, 8192)"
    $CLICKHOUSE_CLIENT --query="CREATE TABLE collapsing_00155 (d Date DEFAULT today(), x UInt64, s Int8 DEFAULT 1) ENGINE = CollapsingMergeTree(d, x, 8192, s)"
    $CLICKHOUSE_CLIENT --query="CREATE TABLE aggregating_00155 (d Date DEFAULT today(), x UInt64, s AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree(d, x, 8192)"
    $CLICKHOUSE_CLIENT --query="CREATE TABLE replacing_00155 (d Date DEFAULT today(), x UInt64, s Int8 DEFAULT 1, v UInt64) ENGINE = ReplacingMergeTree(d, (x), 8192, v)"
}


function cleanup {
    $CLICKHOUSE_CLIENT --query="DROP TABLE summing_00155"
    $CLICKHOUSE_CLIENT --query="DROP TABLE collapsing_00155"
    $CLICKHOUSE_CLIENT --query="DROP TABLE aggregating_00155"
    $CLICKHOUSE_CLIENT --query="DROP TABLE replacing_00155"
}


function test {
    create

    SUM=$(( $1 + $2 ))
    MAX=$(( $1 > $2 ? $1 : $2 ))

    SETTINGS="--min_insert_block_size_rows=0 --min_insert_block_size_bytes=0"

    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO summing_00155 (x) SELECT number AS x FROM system.numbers LIMIT $1"
    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO summing_00155 (x) SELECT number AS x FROM system.numbers LIMIT $2"

    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO collapsing_00155 (x) SELECT number AS x FROM system.numbers LIMIT $1"
    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO collapsing_00155 (x) SELECT number AS x FROM system.numbers LIMIT $2"

    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO aggregating_00155 (d, x, s) SELECT today() AS d, number AS x, sumState(materialize(toUInt64(1))) AS s FROM (SELECT number FROM system.numbers LIMIT $1) GROUP BY number"
    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO aggregating_00155 (d, x, s) SELECT today() AS d, number AS x, sumState(materialize(toUInt64(1))) AS s FROM (SELECT number FROM system.numbers LIMIT $2) GROUP BY number"

    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO replacing_00155 (x, v) SELECT number AS x, toUInt64(number % 3 == 0) FROM system.numbers LIMIT $1"
    $CLICKHOUSE_CLIENT $SETTINGS --query="INSERT INTO replacing_00155 (x, v) SELECT number AS x, toUInt64(number % 3 == 1) FROM system.numbers LIMIT $2"

    $CLICKHOUSE_CLIENT --query="SELECT count() = $SUM, sum(s) = $SUM FROM summing_00155"
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE summing_00155"
    $CLICKHOUSE_CLIENT --query="SELECT count() = $MAX, sum(s) = $SUM FROM summing_00155"
    echo
    $CLICKHOUSE_CLIENT --query="SELECT count() = $SUM, sum(s) = $SUM FROM collapsing_00155"
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE collapsing_00155" --server_logs_file='/dev/null';
    $CLICKHOUSE_CLIENT --query="SELECT count() = $MAX, sum(s) = $MAX FROM collapsing_00155"
    echo
    $CLICKHOUSE_CLIENT --query="SELECT count() = $SUM, sumMerge(s) = $SUM FROM aggregating_00155"
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE aggregating_00155"
    $CLICKHOUSE_CLIENT --query="SELECT count() = $MAX, sumMerge(s) = $SUM FROM aggregating_00155"
    echo
    $CLICKHOUSE_CLIENT --query="SELECT count() = $SUM, sum(s) = $SUM FROM replacing_00155"
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE replacing_00155"
    $CLICKHOUSE_CLIENT --query="SELECT count() = $MAX, sum(s) = $MAX FROM replacing_00155"
    $CLICKHOUSE_CLIENT --query="SELECT count() = sum(v) FROM replacing_00155 where x % 3 == 0 and x < $1"
    $CLICKHOUSE_CLIENT --query="SELECT count() = sum(v) FROM replacing_00155 where x % 3 == 1 and x < $2"
    $CLICKHOUSE_CLIENT --query="SELECT sum(v) = 0 FROM replacing_00155 where x % 3 == 2"
    echo
    echo
}

merged_rows_0=$($CLICKHOUSE_CLIENT -q "select value from system.events where event = 'MergedRows'")

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

merged_rows_1=$($CLICKHOUSE_CLIENT -q "select value from system.events where event = 'MergedRows'")
[[ $merged_rows_1 -le $merged_rows_0 ]]

cleanup
