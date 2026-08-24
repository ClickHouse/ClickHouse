#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This is the regression test for parallel usage of LowCardinality column
# via Buffer engine.
#
# See also:
# - https://github.com/ClickHouse/ClickHouse/issues/24158
# - https://github.com/ClickHouse/ClickHouse/pull/3138

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS low_card_buffer_test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE low_card_buffer_test (test_text LowCardinality(String)) ENGINE=Buffer('', '', 16, 60, 360, 100, 1000, 10000, 100000)"

$CLICKHOUSE_BENCHMARK -d 0 -i 1000 -c 5 <<<"SELECT count() FROM low_card_buffer_test GROUP BY test_text format Null" 2>/dev/null &
$CLICKHOUSE_BENCHMARK -d 0 -i 1000 -c 2 <<<"INSERT INTO low_card_buffer_test values('TEST1')" 2>/dev/null &
wait

# server is alive
$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null"
