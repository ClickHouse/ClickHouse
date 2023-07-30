#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for incorrect mutation of Map() column, see [1].
#
#   [1]: https://github.com/ClickHouse/ClickHouse/issues/30546

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_buffer_map"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_buffer_map(m1 Map(String, UInt64), m2 Map(String, String)) ENGINE = Buffer('', '', 1, 1, 1, 1000000000000, 1000000000000, 1000000000000, 1000000000000)"

# --continue_on_errors -- to ignore possible MEMORY_LIMIT_EXCEEDED errors
$CLICKHOUSE_BENCHMARK --randomize --timelimit 10 --continue_on_errors --concurrency 10 >& /dev/null <<EOL
INSERT INTO t_buffer_map SELECT (range(10), range(10)), (range(10), range(10)) from numbers(100)
SELECT * FROM t_buffer_map
EOL

echo "OK"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_buffer_map"
