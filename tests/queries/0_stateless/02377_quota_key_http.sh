#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

curl -sS "${CLICKHOUSE_URL}" --data "DROP USER IF EXISTS u_02377"
curl -sS "${CLICKHOUSE_URL}" --data "drop quota if exists q_02377"
curl -sS "${CLICKHOUSE_URL}" --data "CREATE USER u_02377 IDENTIFIED WITH plaintext_password BY 'password'"
curl -sS "${CLICKHOUSE_URL}" --data "CREATE QUOTA q_02377 KEYED BY client_key FOR INTERVAL 1 month MAX queries = 100 TO u_02377"

curl -sS -G "${CLICKHOUSE_URL}&user=editor_api&password=password&quota_key=editor_api&query=SELECT%201"

curl -sS "${CLICKHOUSE_URL}" --data "DROP USER IF EXISTS u_02377"
curl -sS "${CLICKHOUSE_URL}" --data "drop quota if exists q_02377"
