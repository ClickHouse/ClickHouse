#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

obf="$CLICKHOUSE_FORMAT --obfuscate"

echo "CREATE TABLE test (secret1 DateTime('UTC'), secret2 DateTime('Europe/Amsterdam')) ENGINE = ReplicatedVersionedCollapsingMergeTree ORDER BY secret1 SETTINGS index_granularity = 8192;" | $obf

echo "SET max_threads = 4" | $obf
echo "SELECT 1 SETTINGS max_threads = 4, max_memory_usage = 10000000000" | $obf
echo "INSERT INTO t VALUES (1, 'hello')" | $obf
printf "INSERT INTO t FORMAT CSV\n1,hello\n" | $obf
echo "INSERT INTO t SELECT 1" | $obf
echo "SELECT 1 FORMAT Pretty" | $obf
echo "SELECT inf, nan, -inf, -nan" | $obf
echo "SELECT INTERVAL '2 years'" | $obf
echo "SELECT '192.168.1.100'" | $obf
echo "SELECT '550e8400-e29b-41d4-a716-446655440000'" | $obf
