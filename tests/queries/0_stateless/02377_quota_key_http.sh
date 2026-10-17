#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
quota="q_${CLICKHOUSE_TEST_UNIQUE_NAME}"

curl -sS "${CLICKHOUSE_URL}" --data "DROP USER IF EXISTS ${user}"
curl -sS "${CLICKHOUSE_URL}" --data "drop quota if exists ${quota}"
curl -sS "${CLICKHOUSE_URL}" --data "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
curl -sS "${CLICKHOUSE_URL}" --data "CREATE QUOTA ${quota} KEYED BY client_key FOR INTERVAL 1 month MAX queries = 100 TO ${user}"

curl -sS -G "${CLICKHOUSE_URL}&user=${user}&password=password&quota_key=${quota}&query=SELECT%201"

curl -sS "${CLICKHOUSE_URL}" --data "DROP USER IF EXISTS ${user}"
curl -sS "${CLICKHOUSE_URL}" --data "drop quota if exists ${quota}"
