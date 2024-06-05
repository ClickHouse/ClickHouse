#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "CREATE TABLE test_cc (d Dynamic) ENGINE = Memory"


$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "INSERT INTO test_cc SELECT number::Int64 AS d FROM numbers(10000) SETTINGS max_threads=1,max_insert_threads=1" &
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "INSERT INTO test_cc SELECT toString(number) AS d FROM numbers(10000) SETTINGS max_threads=2,max_insert_threads=2" &
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "INSERT INTO test_cc SELECT toDate(number % 10000) AS d FROM numbers(10000) SETTINGS max_threads=3,max_insert_threads=3" &
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "INSERT INTO test_cc SELECT [number, number + 1] AS d FROM numbers(10000) SETTINGS max_threads=4,max_insert_threads=4" &
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "INSERT INTO test_cc SELECT toFloat64(number) AS d FROM numbers(10000) SETTINGS max_threads=5,max_insert_threads=5" &
$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "INSERT INTO test_cc SELECT map(number, toString(number)) AS d FROM numbers(10000) SETTINGS max_threads=6,max_insert_threads=6" &

$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 --use_variant_as_common_type=1 --allow_experimental_variant_type=1 -q "INSERT INTO test_cc SELECT CAST(multiIf(number % 5 = 0, toString(number), number % 5 = 1, number, number % 5 = 2, toFloat64(number), number % 5 = 3, toDate('2020-01-01'), [number, number + 1]), 'Dynamic') FROM numbers(10000) SETTINGS max_threads=6,max_insert_threads=6" &

wait

$CLICKHOUSE_CLIENT --allow_experimental_dynamic_type=1 -q "SELECT dynamicType(d) t, count(), uniqExact(d) FROM test_cc GROUP BY t ORDER BY t"
