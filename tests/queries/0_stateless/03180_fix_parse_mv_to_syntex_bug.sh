#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&param_target=mv_target&param_src=mv_src&param_mv_name=mv&param_mv_db_name=test_db"
CLICKHOUSE_CLIENT="$1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv_target"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS test_db"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE test_db"

${CLICKHOUSE_CURL} -sS ${url} -d "CREATE OR REPLACE TABLE {target: Identifier} (i Int32) ENGINE MergeTree ORDER BY i;"
${CLICKHOUSE_CURL} -sS ${url} -d "CREATE OR REPLACE TABLE {src: Identifier} (i Int32) ENGINE MergeTree ORDER BY i;"
${CLICKHOUSE_CURL} -sS ${url} -d "CREATE MATERIALIZED VIEW {mv_name: Identifier} TO {target: Identifier}  AS SELECT * FROM {src: Identifier};"
${CLICKHOUSE_CLIENT} -q "INSERT INTO mv_src VALUES (1)"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM mv_src ORDER BY id"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM mv ORDER BY id"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM mv_target ORDER BY id"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv"

${CLICKHOUSE_CURL} -sS ${url} -d "CREATE MATERIALIZED VIEW {mv_db_name: Identifier}.{mv_name: Identifier} TO {target: Identifier}  AS SELECT * FROM {src: Identifier};"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS mv_target"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS test_db"
