#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# just stop, no exception
${CLICKHOUSE_CLIENT} --query="#" 2>&1 | grep -F -q 'Syntax error' && echo 'OK' || echo 'FAIL'
${CLICKHOUSE_CLIENT} --query="#not a comemnt" 2>&1 | grep -F -q 'Syntax error' && echo 'OK' || echo 'FAIL'
# syntax error
${CLICKHOUSE_CLIENT} --query="select 1 #not a comemnt" 2>&1 | grep -F -q 'Syntax error' && echo 'OK' || echo 'FAIL'
${CLICKHOUSE_CLIENT} --query="select 1 #" 2>&1 | grep -F -q 'Syntax error' && echo 'OK' || echo 'FAIL'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "select 42 #" 2>&1 | grep -F -q 'Syntax error' && echo 'OK' || echo 'FAIL'
