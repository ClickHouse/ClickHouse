#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# it is the worst way of making performance test, nevertheless it can detect significant slowdown and some other issues, that usually found by stress test

db="test_01193_$RANDOM"

declare -A engines
engines[0]="Memory"
engines[1]="File(CSV)"
engines[2]="Log"
engines[3]="StripeLog"
engines[4]="MergeTree ORDER BY i"

tables=1000
threads=10
count_multiplier=1
max_time_ms=1000

debug_or_sanitizer_build=$($CLICKHOUSE_CLIENT -q "WITH ((SELECT value FROM system.build_options WHERE name='BUILD_TYPE') AS build, (SELECT value FROM system.build_options WHERE name='CXX_FLAGS') as flags) SELECT build='Debug' OR flags LIKE '%fsanitize%' OR hasThreadFuzzer()")

if [[ debug_or_sanitizer_build -eq 1 ]]; then tables=100; count_multiplier=10; max_time_ms=1500; fi

create_tables() {
  for i in $(seq 1 $tables); do
    engine=${engines[$((i % ${#engines[@]}))]}
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $db.table_$1_$i (i UInt64, d Date, s String, n Nested(i UInt8, f Float32)) ENGINE=$engine"
    $CLICKHOUSE_CLIENT -q "INSERT INTO $db.table_$1_$i VALUES (0, '2020-06-25', 'hello', [1, 2], [3, 4]), (1, '2020-06-26', 'word', [10, 20], [30, 40])"
  done
}

$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db"

for i in $(seq 1 $threads); do
  create_tables "$i" &
done
wait

$CLICKHOUSE_CLIENT -q "CREATE TABLE $db.table_merge (i UInt64, d Date, s String, n Nested(i UInt8, f Float32)) ENGINE=Merge('$db', '^table_')"
$CLICKHOUSE_CLIENT -q "SELECT count() * $count_multiplier, i, d, s, n.i, n.f FROM $db.table_merge GROUP BY i, d, s, n.i, n.f ORDER BY i"

db_engine=$($CLICKHOUSE_CLIENT -q "SELECT engine FROM system.databases WHERE name='$db'")

$CLICKHOUSE_CLIENT -q "DETACH DATABASE $db"

# get real time, grep seconds, remove point, remove leading zeros
elapsed_ms=$({ time $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $db ENGINE=$db_engine"; } 2>&1 | grep real | grep -Po "0m\K[0-9\.]*" | tr -d '.' | sed "s/^0*//")
$CLICKHOUSE_CLIENT -q "SELECT '01193_metadata_loading', $elapsed_ms FORMAT Null" # it will be printed to server log

if [[ $elapsed_ms -le $max_time_ms ]]; then echo ok; fi

$CLICKHOUSE_CLIENT -q "SELECT count() * $count_multiplier, i, d, s, n.i, n.f FROM $db.table_merge GROUP BY i, d, s, n.i, n.f ORDER BY i"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $db"
