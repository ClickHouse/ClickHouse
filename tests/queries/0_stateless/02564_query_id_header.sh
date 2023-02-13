#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_query_id_header"

${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}" -d "CREATE TABLE t_query_id_header (a UInt64) ENGINE = Memory" 2>&1 | grep -o "X-ClickHouse-Query-Id"
${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}" -d "INSERT INTO t_query_id_header VALUES (1)" 2>&1 | grep -o "X-ClickHouse-Query-Id"
${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}" -d "EXISTS TABLE t_query_id_header" 2>&1 | grep -o "X-ClickHouse-Query-Id"
${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}" -d "SELECT * FROM t_query_id_header" 2>&1 | grep -o "X-ClickHouse-Query-Id"
${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}" -d "DROP TABLE t_query_id_header" 2>&1 | grep -o "X-ClickHouse-Query-Id"
