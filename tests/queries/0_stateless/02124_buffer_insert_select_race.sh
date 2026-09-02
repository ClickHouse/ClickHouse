#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for 'Logical error: No column to rollback' in case of
# exception while commiting batch into the Buffer, see [1].
#
#   [1]: https://github.com/ClickHouse/ClickHouse/issues/42740

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_buffer_string"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_buffer_string(key String) ENGINE = Buffer('', '', 1, 1, 1, 1000000000000, 1000000000000, 1000000000000, 1000000000000)"

# --continue_on_errors -- to ignore possible MEMORY_LIMIT_EXCEEDED errors
# --concurrency -- we need have SELECT and INSERT in parallel to have refcount
#                  of the column in the Buffer block > 1, that way we will do
#                  full clone and moving a column may throw.
#
# It reproduces the problem 100% with MemoryTrackerFaultInjectorInThread in the appendBlock()
$CLICKHOUSE_BENCHMARK --randomize --timelimit 10 --continue_on_errors --concurrency 10 >& /dev/null <<EOL
INSERT INTO t_buffer_string SELECT number::String from numbers(10000)
SELECT * FROM t_buffer_string
EOL

$CLICKHOUSE_CLIENT -q "DROP TABLE t_buffer_string"
