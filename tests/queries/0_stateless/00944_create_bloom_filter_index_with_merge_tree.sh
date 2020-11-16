#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

for sequence in 1 10 100 1000 10000 100000 1000000 10000000 100000000 1000000000; do \
rate=$(echo "1 $sequence" | awk '{printf("%0.9f\n",$1/$2)}')
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.bloom_filter_idx";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.bloom_filter_idx ( u64 UInt64, i32 Int32, f64 Float64, d Decimal(10, 2), s String, e Enum8('a' = 1, 'b' = 2, 'c' = 3), dt Date, INDEX bloom_filter_a i32 TYPE bloom_filter($rate) GRANULARITY 1 ) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192"
done
