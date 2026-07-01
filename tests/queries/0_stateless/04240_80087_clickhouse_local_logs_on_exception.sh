#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/80087
# With send_logs_level set in clickhouse-local, logs accumulated before a query
# failure must still be printed; previously the exception was sent without
# flushing the buffered log queue.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query that fails fast during analysis. With send_logs_level=trace, a few
# log lines are produced before the failure (executeQuery, ContextAccess, ...).
output=$($CLICKHOUSE_LOCAL --send_logs_level=trace -q "SELECT * FROM nonexistent_table_xyz_04240" 2>&1)

echo "$output" | grep -qE '<(Debug|Trace)>' && echo "logs printed: ok"
echo "$output" | grep -q "UNKNOWN_TABLE" && echo "exception printed: ok"
