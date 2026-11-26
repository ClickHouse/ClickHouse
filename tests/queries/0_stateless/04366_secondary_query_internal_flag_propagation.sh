#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary reload runs as an internal query (is_internal=1).
# Its ClickHouse source query reads through remote(), which dispatches a secondary query.
# The secondary query must inherit is_internal=1.

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY dict (n UInt64)
PRIMARY KEY n
SOURCE(CLICKHOUSE(QUERY 'SELECT number AS n FROM remote(''127.0.0.2'', numbers(1)) SETTINGS log_comment=''$CLICKHOUSE_TEST_UNIQUE_NAME'', prefer_localhost_replica=0'))
LIFETIME(MIN 100500 MAX 100500)
LAYOUT(FLAT())"

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY dict"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.query_log
WHERE is_initial_query = 0 AND is_internal = 1 AND log_comment = '$CLICKHOUSE_TEST_UNIQUE_NAME'
  AND current_database IN ['default', currentDatabase()]"
