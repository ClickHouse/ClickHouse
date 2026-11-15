#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_parquet_uuid"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_parquet_uuid (id UUID) ENGINE = Memory"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_parquet_uuid VALUES ('2ea7cbb5-5bd2-4bfc-9152-e37f730e5a46'), ('6915af5b-e468-417e-80ee-a9280a6d2c20'), ('ffffffff-ffff-ffff-ffff-ffffffffffff')"

# Export via Parquet, pipe the binary output, and parse it back into UUIDs.
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_parquet_uuid FORMAT Parquet" | \
    $CLICKHOUSE_LOCAL --input-format Parquet --structure "id UUID" -q "SELECT * FROM table"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_parquet_uuid"
