#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "drop table if exists test_progress"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "create table test_progress (x UInt64, y UInt64, d Date, a Array(UInt64), s String) engine=MergeTree() order by x"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "insert into test_progress select number as x, number + 1 as y, toDate(number) as d, range(number % 10) as a, repeat(toString(number), 10) as s from numbers(200000)"
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "SELECT * from test_progress FORMAT JSONEachRowWithProgress" | grep -v --text "progress" | wc -l
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "drop table test_progress";
