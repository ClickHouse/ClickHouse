#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1" -d @- <<< "DROP TABLE IF EXISTS test_00210"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1" -d @- <<< "CREATE TABLE test_00210 (x UInt8) ENGINE = Log"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1" -d @- <<< "INSERT INTO test_00210 SELECT 1 AS x"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&extremes=1" -d @- <<< "DROP TABLE test_00210"
