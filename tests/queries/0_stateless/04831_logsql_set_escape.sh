#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The SQL SET escape of the logsql dialect applies only to a complete standalone SET
# statement: a LogsQL query merely starting with the word `set` keeps its word-filter
# meaning, and the escape works even after a leading LogsQL '#' comment.

$CLICKHOUSE_CLIENT -q "CREATE TABLE logs_04831 (\`_time\` DateTime, \`_msg\` String) ENGINE = MergeTree ORDER BY _time"
$CLICKHOUSE_CLIENT -q "INSERT INTO logs_04831 VALUES ('2024-01-01 00:00:00', 'cannot set error handler'), ('2024-01-01 01:00:00', 'unrelated message')"

LOGSQL_URL="${CLICKHOUSE_URL}&dialect=logsql&allow_experimental_logsql_dialect=1&logsql_table=logs_04831"

# A LogsQL query starting with the word `set` is a word filter, not a SQL SET.
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "set error | count()"
${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "set"

# A complete standalone SET is still parsed as SQL, even when the dialect is not usable:
# with the feature gate off and no logsql_table, the escape must keep working.
GATED_URL="${CLICKHOUSE_URL}&dialect=logsql"
${CLICKHOUSE_CURL} -sS "$GATED_URL" --data-binary "SET dialect = 'clickhouse'" && echo "standalone SET ok"

# The escape also works after a leading LogsQL '#' comment, which the ClickHouse
# lexer cannot tokenize.
${CLICKHOUSE_CURL} -sS "$GATED_URL" --data-binary "# switch back to SQL
SET dialect = 'clickhouse'" && echo "SET after comment ok"

# LogsQL comments are valid after the SET too, including after its semicolon.
${CLICKHOUSE_CURL} -sS "$GATED_URL" --data-binary "SET dialect = 'clickhouse' #switch-back" && echo "SET before comment ok"
${CLICKHOUSE_CURL} -sS "$GATED_URL" --data-binary "SET dialect = 'clickhouse'; #switch-back" && echo "SET before semicolon comment ok"

# An incomplete SET (with trailing LogsQL) is not stolen by the escape: it is parsed
# as LogsQL and hits the feature gate.
${CLICKHOUSE_CURL} -sS "$GATED_URL" --data-binary "set error | count()" |& grep -om1 "allow_experimental_logsql_dialect"

# The client-side parser follows the same rules.
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table logs_04831 --dialect logsql -q "set error | count()"
$CLICKHOUSE_CLIENT --dialect logsql -q "SET dialect = 'clickhouse'" && echo "client standalone SET ok"

# A LogsQL comment suffix after the semicolon belongs to the SET statement.
$CLICKHOUSE_CLIENT --dialect logsql -q "SET dialect = 'clickhouse'; #switch-back" && echo "client SET semicolon comment ok"

# In multiquery mode the statement separator stays visible, so the query following
# the SET escape is executed with the dialect it has just switched to.
printf "SET dialect = 'clickhouse';\nSELECT 1;\n" | $CLICKHOUSE_CLIENT --multiquery --dialect logsql && echo "client SET multiquery ok"

# A LogsQL query that merely starts with the word `set` is a word filter, even when
# it forms a valid shorthand `SET name` statement in SQL.
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table logs_04831 --dialect logsql -q "set error"

$CLICKHOUSE_CLIENT -q "DROP TABLE logs_04831"
