#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
quota="q_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "drop quota if exists ${quota}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password';"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY client_key FOR INTERVAL 1 month MAX queries = 100 TO ${user};"

${CLICKHOUSE_CLIENT} --user="${user}" --password=password --quota_key="${quota}" --query="select 1"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "drop quota if exists ${quota}"
