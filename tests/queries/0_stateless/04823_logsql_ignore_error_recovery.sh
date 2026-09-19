#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under --ignore-error, a failing LogsQL statement is skipped to the next ';'
# with the lexer of the dialect itself. The plain ClickHouse lexer would stop
# at the first LogsQL-only token (like a leading '!') and resume parsing in
# the middle of the failed statement, executing its mutilated tail.

QUERIES_FILE=$CLICKHOUSE_TMP/logsql_ignore_error_04823.sql
cat > "$QUERIES_FILE" <<'EOF'
SET dialect = 'clickhouse';
CREATE TABLE logs_04823 (`_time` DateTime64(9, 'UTC'), `_msg` String) ENGINE = MergeTree ORDER BY _time;
INSERT INTO logs_04823 VALUES ('2024-01-01', 'hello');
SET dialect = 'logsql', allow_experimental_logsql_dialect = 1, logsql_table = 'logs_04823';
!x | bogus_pipe "unterminated ; SELECT 2" ; * | stats count();
="a;b" | another_bogus_pipe ; * | stats count();
* | stats count();
EOF

$CLICKHOUSE_LOCAL --ignore-error --queries-file "$QUERIES_FILE" < /dev/null 2>/dev/null

rm "$QUERIES_FILE"
