#!/usr/bin/env bash
# Tags: long

# This test was split in two due to long runtimes in sanitizers.
# The other part is 02099_tsv_raw_format_1.sh.
#
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_parallel_parsing_02099"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_parallel_parsing_02099 (x UInt64, a Array(UInt64), s String) ENGINE=Memory()";
$CLICKHOUSE_CLIENT -q "SELECT number AS x, range(number % 50) AS a, toString(a) AS s FROM numbers(100000) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT --input_format_parallel_parsing=0 -q "INSERT INTO test_parallel_parsing_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_parallel_parsing_02099 ORDER BY x" | md5sum

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_parallel_parsing_02099"

$CLICKHOUSE_CLIENT -q "SELECT number AS x, range(number % 50) AS a, toString(a) AS s FROM numbers(100000) FORMAT TSVRaw" | $CLICKHOUSE_CLIENT --input_format_parallel_parsing=1 -q "INSERT INTO test_parallel_parsing_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_parallel_parsing_02099 ORDER BY x" | md5sum

$CLICKHOUSE_CLIENT -q "DROP TABLE test_parallel_parsing_02099"

