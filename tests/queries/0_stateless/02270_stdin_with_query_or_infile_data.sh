#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_02270"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02270 (x UInt32) ENGINE=Memory"

echo "(42)" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_02270 FORMAT Values (24)"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02270 ORDER BY x"

echo "(24)" > 02270_data.values
echo "(42)" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_02270 FROM INFILE '02270_data.values'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02270 ORDER BY x"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_02270"
rm 02270_data.values
