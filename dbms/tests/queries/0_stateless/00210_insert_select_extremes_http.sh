#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL}?extremes=1 -d @- <<< "DROP TABLE IF EXISTS test.test"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL}?extremes=1 -d @- <<< "CREATE TABLE test.test (x UInt8) ENGINE = Log"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL}?extremes=1 -d @- <<< "INSERT INTO test.test SELECT 1 AS x"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL}?extremes=1 -d @- <<< "DROP TABLE test.test"
