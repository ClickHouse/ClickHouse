#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_01903"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_01903 (col0 Date, col1 Nullable(UInt8)) ENGINE MergeTree() PARTITION BY toYYYYMM(col0) ORDER BY col0;"

# Use 100k rows: enough to exercise multi-block streaming in the CSVWithNames/TSVWithNames
# row-format readers (default max_block_size = 65505) while keeping the test well under the
# 180s flaky-check timeout under sanitizer + WasmEdge runs. The original 1M rows added no
# extra coverage but was prone to timing out under MSan + WasmEdge (see CIDB).
$CLICKHOUSE_CLIENT -q "INSERT INTO test_01903 FORMAT CSVWithNames" < <(
    echo 'col0,col1'
    yes '2021-05-05,1' | head -n 100000
)

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01903"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_01903 (col0) FORMAT CSVWithNames" < <(
    echo 'col0'
    yes '2021-05-05' | head -n 100000
)

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01903"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_01903 (col0) FORMAT TSVWithNames" < <(
    echo 'col0'
    yes '2021-05-05' | head -n 100000
)

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_01903"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_01903"
