#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The LogsQL text is scanned from the raw query string, bypassing the ClickHouse token stream,
# so the max_query_size limit must be enforced on that raw scan as well.

$CLICKHOUSE_CLIENT -q "CREATE TABLE logs_04818 (\`_time\` DateTime, \`_msg\` String) ENGINE = MergeTree ORDER BY _time"

LOGSQL_URL="${CLICKHOUSE_URL}&dialect=logsql&allow_experimental_logsql_dialect=1&logsql_table=logs_04818&max_query_size=50"

# A query within the limit works.
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "error | count()"

LONG_WORD=$(printf 'a%.0s' {1..100})

# An oversized query is rejected instead of being fully scanned, in a long unquoted token,
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "_msg:${LONG_WORD}" |& grep -om1 "Max query size exceeded"
# in an unterminated quoted string,
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "_msg:\"${LONG_WORD}" |& grep -om1 "Max query size exceeded"
# and in a query of many short tokens.
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "$(printf 'a %.0s' {1..50})b" |& grep -om1 "Max query size exceeded"

# The client enforces the same limit when it parses the dialect itself.
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table logs_04818 --dialect logsql --max_query_size 50 \
    -q "_msg:${LONG_WORD}" |& grep -om1 "Max query size exceeded"

$CLICKHOUSE_CLIENT -q "DROP TABLE logs_04818"
