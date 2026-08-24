#!/usr/bin/env bash
set -e

clickhouse-client -q "DROP TABLE IF EXISTS test.comparisons"
clickhouse-client -q "CREATE TABLE test.comparisons (i64 Int64, u64 UInt64, f64 Float64) ENGINE = Memory"
clickhouse-client -q "INSERT INTO test.comparisons SELECT toInt64(rand64()) + number AS i64, number AS u64, reinterpretAsFloat64(reinterpretAsString(rand64())) AS f64 FROM system.numbers LIMIT 90000000"

function test_cmp {
    echo -n "$1 : "
    echo "SELECT count() FROM test.comparisons WHERE ($1)" | clickhouse-benchmark --max_threads=1 -i 20 -d 0 --json test.json 1>&2 2>/dev/null
    python3 -c "import json; print '%.3f' % float(json.load(open('test.json'))['query_time_percentiles']['0'])"
    rm test.json
}

test_cmp "u64 > i64"
test_cmp "u64 > toInt64(1)"
test_cmp "i64 > u64"
test_cmp "i64 > toUInt64(1)"

test_cmp "u64 = i64"
test_cmp "u64 = toInt64(1)"
test_cmp "i64 = u64"
test_cmp "i64 = toUInt64(1)"

test_cmp "u64 >= i64"
test_cmp "i64 > -1"
test_cmp "i64 = 0"
test_cmp "u64 != 0"

test_cmp "i64 = f64"
test_cmp "i64 < f64"
test_cmp "f64 >= 0"

clickhouse-client -q "DROP TABLE IF EXISTS test.comparisons"
