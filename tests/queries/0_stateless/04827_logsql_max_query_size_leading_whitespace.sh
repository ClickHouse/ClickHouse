#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The max_query_size budget of the raw LogsQL scan is measured from the raw query begin,
# so leading whitespace and comments cannot extend the budget.

$CLICKHOUSE_CLIENT -q "CREATE TABLE logs_04827 (\`_time\` DateTime, \`_msg\` String) ENGINE = MergeTree ORDER BY _time"

LOGSQL_URL="${CLICKHOUSE_URL}&dialect=logsql&allow_experimental_logsql_dialect=1&logsql_table=logs_04827&max_query_size=50"

# A query within the limit works, also with a leading-whitespace prefix that still fits.
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "error | count()"
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "   error | count()"

# 10 leading spaces plus a 45-byte word exceed the 50-byte budget together,
# even though the word alone would fit.
TEN_SPACES='          '
WORD_45=$(printf 'a%.0s' {1..45})
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "${TEN_SPACES}_msg:${WORD_45}" |& grep -om1 "Max query size exceeded"

# A leading comment counts toward the budget as well.
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "-- padding padding padding padding padding
error | count()" |& grep -om1 "Max query size exceeded"

# Whitespace alone longer than the budget is rejected too.
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "$(printf ' %.0s' {1..60})error" |& grep -om1 "Max query size exceeded"

# The client enforces the same accounting when it parses the dialect itself
# (the client trims leading whitespace before parsing, so a comment prefix is used).
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table logs_04827 --dialect logsql --max_query_size 50 \
    -q "-- padding padding padding padding padding
error | count()" |& grep -om1 "Max query size exceeded"

$CLICKHOUSE_CLIENT -q "DROP TABLE logs_04827"
