#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `max_query_size` budget of the standalone SET escape of the logsql dialect is
# measured from the raw query begin as well, so the LogsQL `#` comments and whitespace
# that the escape skips before re-tokenizing the statement count toward it.

$CLICKHOUSE_CLIENT -q "CREATE TABLE logs_04842 (\`_time\` DateTime, \`_msg\` String) ENGINE = MergeTree ORDER BY _time"

# A 48-byte comment plus a 21-byte SET statement: 70 raw bytes in total.
COMMENT='# padding padding padding padding padding padding'
STATEMENT="SET max_threads = 4;"

# The escape works when the whole raw text fits into the budget.
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table logs_04842 --dialect logsql --max_query_size 100 \
    -q "$COMMENT
$STATEMENT" && echo "SET after comment within the budget ok"

# The same statement is rejected once the comment prefix pushes it over the budget,
# even though the SET statement alone is only 21 bytes long.
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table logs_04842 --dialect logsql --max_query_size 60 \
    -q "$COMMENT
$STATEMENT" |& grep -om1 "Max query size exceeded"

# A comment prefix longer than the whole budget is rejected too.
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table logs_04842 --dialect logsql --max_query_size 20 \
    -q "$COMMENT
$STATEMENT" |& grep -om1 "Max query size exceeded"

# Over HTTP the accounting is the same.
LOGSQL_URL="${CLICKHOUSE_URL}&dialect=logsql&allow_experimental_logsql_dialect=1&logsql_table=logs_04842"
${CLICKHOUSE_CURL} -sS "${LOGSQL_URL}&max_query_size=100" --data-binary "$COMMENT
$STATEMENT" && echo "HTTP SET after comment within the budget ok"
${CLICKHOUSE_CURL} -sS "${LOGSQL_URL}&max_query_size=60" --data-binary "$COMMENT
$STATEMENT" |& grep -om1 "Max query size exceeded"

$CLICKHOUSE_CLIENT -q "DROP TABLE logs_04842"
