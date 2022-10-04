#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<< "drop table if exists insert_select_progress_http"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<< "create table insert_select_progress_http(n UInt16) engine = MergeTree order by n"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1" -d @- <<< "insert into insert_select_progress_http select * from numbers(1e3)" -v |& grep X-ClickHouse-Summary

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<< "drop table insert_select_progress_http"
