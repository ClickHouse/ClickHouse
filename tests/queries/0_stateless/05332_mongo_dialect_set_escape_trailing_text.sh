#!/usr/bin/env bash
# The `SET` escape of the `mongo` dialect has to be the whole statement: `ParserSetQuery`
# succeeds on a prefix, so `SET dialect = 'clickhouse' garbage` used to run the `SET` and either
# drop the rest or report it as a second statement. It is a syntax error now, both in the
# multi-statement parser of the client and in the single-statement parser of the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MONGO="--allow_experimental_mongo_dialect=1 --dialect=mongo"

# Only the error code is printed, because the test runner rejects the word `Exception` in the
# standard output of a test.
${CLICKHOUSE_LOCAL} $MONGO --query "SET dialect = 'clickhouse' garbage" 2>&1 | grep -o -m1 'SYNTAX_ERROR'
${CLICKHOUSE_LOCAL} $MONGO --query "SET dialect = 'clickhouse' garbage; SELECT 1" 2>&1 | grep -o -m1 'SYNTAX_ERROR'

# The server parses the text of the query again with the dialect of the session.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_mongo_dialect=1&dialect=mongo" \
    --data-binary "SET dialect = 'clickhouse' garbage" | grep -o -m1 'SYNTAX_ERROR'

# A `SET` with nothing but its terminator after it still switches the dialect back.
${CLICKHOUSE_LOCAL} $MONGO --query "SET dialect = 'clickhouse'; SELECT 1"
${CLICKHOUSE_LOCAL} $MONGO --query "SET dialect = 'clickhouse';"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_mongo_dialect=1&dialect=mongo" \
    --data-binary "SET dialect = 'clickhouse';"
