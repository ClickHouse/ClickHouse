#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client parses the query with the dialect gate of the session and applies the query's own
# `SETTINGS` before sending it. The receiving side must parse the same text with the gate the client
# accepted it with; the query still runs with its own `SETTINGS`.
$CLICKHOUSE_CLIENT --enable_trino_dialect 1 --dialect trino -q "SELECT 1 SETTINGS enable_trino_dialect = 0"
$CLICKHOUSE_LOCAL --enable_trino_dialect 1 --dialect trino -q "SELECT 2 SETTINGS enable_trino_dialect = 0"

# A query-level `SETTINGS` cannot open the gate for the text it belongs to.
$CLICKHOUSE_CLIENT --dialect trino -q "SELECT 3 SETTINGS enable_trino_dialect = 1" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
$CLICKHOUSE_LOCAL --dialect trino -q "SELECT 4 SETTINGS enable_trino_dialect = 1" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
