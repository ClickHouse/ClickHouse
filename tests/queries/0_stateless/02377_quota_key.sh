#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_02377"
${CLICKHOUSE_CLIENT} -q "drop quota if exists q_02377"
${CLICKHOUSE_CLIENT} -q "CREATE USER u_02377 IDENTIFIED WITH plaintext_password BY 'password';"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q_02377 KEYED BY client_key FOR INTERVAL 1 month MAX queries = 100 TO u_02377;"

${CLICKHOUSE_CLIENT} --user=u_02377 --password=password --quota_key=q_02377 --query="select 1"
curl -sS "${CLICKHOUSE_URL}&user=editor_api&password=password&quota_key=editor_api&query=SELECT%201"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_02377"
${CLICKHOUSE_CLIENT} -q "drop quota if exists q_02377"
