#!/usr/bin/env bash
# Tags: no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

insert_opts=(
    "--min_insert_block_size_rows=1"
    "--max_block_size=1"
)

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_q"
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "CREATE TABLE test_q (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS queue_mode=1"

echo "start parallel insert"
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO test_q (*) select number, number from numbers(100)" &
$CLICKHOUSE_CLIENT "${insert_opts[@]}" -q "INSERT INTO test_q (*) select number, number from numbers(100)" &

echo "waiting insert jobs completion"
wait

$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM test_q"
$CLICKHOUSE_CLIENT -q "SELECT min(_queue_block_number), max(_queue_block_number) FROM test_q"

echo "optimize table"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE test_q"
$CLICKHOUSE_CLIENT -q "SELECT count(DISTINCT _part) < 200 FROM test_q"
