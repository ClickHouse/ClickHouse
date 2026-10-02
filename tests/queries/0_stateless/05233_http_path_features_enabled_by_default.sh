#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The path-as-URL features of the HTTP interface are enabled by default since 26.10:
# the server-level `http_allow_path_requests` and the per-user `http_allow_database_as_path`,
# `http_allow_table_as_file` and `http_allow_filters_as_path`. None of the requests below pass any
# of them, so each one only works if the default is on. `http_allow_filters_as_unrecognized_url_parameters`
# stays off by default, because it would turn any unknown URL parameter of any request into a filter.

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.hits (a UInt32, b String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.hits VALUES (1, 'one'), (2, 'two'), (3, 'three')"

echo "-- table as file"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/${DB}/hits.CSV"

echo "-- database as path"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/${DB}/hits.TSV?select=b&order=a"

echo "-- filters as path"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/${DB}/a=2/hits.CSV"

echo "-- filters as unrecognized URL parameters are off by default"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/${DB}/hits.CSV?b=three" | grep -o 'UNKNOWN_SETTING'

echo "-- filters as unrecognized URL parameters can be switched on per request"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/${DB}/hits.CSV?http_allow_filters_as_unrecognized_url_parameters=1&b=three"

echo "-- the features can still be switched off per request"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/${DB}/hits.CSV?http_allow_table_as_file=0" | grep -o 'UNKNOWN_TABLE'
