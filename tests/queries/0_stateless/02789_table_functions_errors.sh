#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Simply calling a table function correctly should not raise any error (in particular not
# UNKNOWN_TABLE). Check this query's own query_log row instead of the process-wide
# system.errors counter, which any concurrent UNKNOWN_TABLE-triggering test would perturb.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "SELECT count() FROM numbers(10)"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "SELECT exception_code = 0 FROM system.query_log WHERE query_id = '$query_id' AND type != 'QueryStart' AND current_database = currentDatabase()"
