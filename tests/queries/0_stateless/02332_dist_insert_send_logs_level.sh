#!/usr/bin/env bash
# Tags: no-backward-compatibility-check

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --server_logs_file /dev/null -q "CREATE TABLE data_02332 (key Int) Engine=Null()"
# If ClickHouse server will forward logs from the remote nodes, than it will definitely will have the following message in the log:
#
#     <Debug> executeQuery: (from 127.0.0.1:53440, initial_query_id: fc1f7dbd-845b-4142-9306-158ddd564e61) INSERT INTO default.data (key) VALUES (stage: Complete)
#
# And if the server will forward logs, then the query may hung.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION remote('127.2', currentDatabase(), data_02332) SELECT * FROM numbers(10)" |& grep 'executeQuery.*initial_query_id.*INSERT INTO'
exit 0
