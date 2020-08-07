#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-User: default' -d 'SELECT 1'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-User: header_test' -d 'SELECT 1' | grep -o 'Code: 516'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Key: ' -d 'SELECT 1'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Key: header_test' -d 'SELECT 1' | grep -o 'Code: 516'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Quota: ' -d 'SELECT 1'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Quota: header_test' -d 'SELECT 1' | grep -o 'Code: 516'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Database: system' -d 'SHOW TABLES' | grep -o 'processes'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Database: header_test' -d 'SHOW TABLES' | grep -o 'Code: 81'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Format: JSONCompactEachRow' -d 'SELECT 1' | grep -o '\[1\]'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Format: header_test' -d 'SELECT 1' | grep -o 'Code: 73'
