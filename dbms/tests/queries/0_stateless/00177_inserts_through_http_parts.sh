#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=DROP+TABLE" -d 'IF EXISTS insert'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=CREATE" -d 'TABLE insert (x UInt8) ENGINE = Memory'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'INSERT INTO insert VALUES (1),(2)'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+insert+VALUES" -d '(3),(4)'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+insert" -d 'VALUES (5),(6)'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+insert+VALUES+(7)" -d ',(8)'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=INSERT+INTO+insert+VALUES+(9),(10)" -d ' '
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT x FROM insert ORDER BY x'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=DROP+TABLE" -d 'insert'
