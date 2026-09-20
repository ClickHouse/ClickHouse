#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A non-table-function expression in the database/db override of a named collection must be
# rejected while parsing the arguments, not reach TableFunctionFactory::get() as a null AST.

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc"

${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS \`${nc}\`"
${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION \`${nc}\` AS host = '127.0.0.1', port = 9000, table = 'one'"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM remote(\`${nc}\`, database = (SELECT 1))" 2>&1 | grep -o -m1 "BAD_GET"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM remote(\`${nc}\`, db = (SELECT 1))" 2>&1 | grep -o -m1 "BAD_GET"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM remote(\`${nc}\`, database = 1)" 2>&1 | grep -o -m1 "BAD_GET"

# The server is still responsive after the rejected queries.
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION \`${nc}\`"
