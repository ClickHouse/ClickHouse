#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

prefix="test_db_"
db_name="${prefix}${RANDOM}"
url="${CLICKHOUSE_URL}&param_target=mv_target&param_src=mv_src&param_mv_name=mv&param_mv_db_name=${db_name}"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db_name}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db_name}"

${CLICKHOUSE_CURL} -sS "$url" -d "CREATE OR REPLACE TABLE {mv_db_name: Identifier}.{target: Identifier} (i Int32) ENGINE MergeTree ORDER BY i;"
${CLICKHOUSE_CURL} -sS "$url" -d "CREATE OR REPLACE TABLE {mv_db_name: Identifier}.{src: Identifier} (i Int32) ENGINE MergeTree ORDER BY i;"
${CLICKHOUSE_CURL} -sS "$url" -d "CREATE MATERIALIZED VIEW {mv_db_name: Identifier}.{mv_name: Identifier} TO {mv_db_name: Identifier}.{target: Identifier}  AS SELECT * FROM {mv_db_name: Identifier}.{src: Identifier};"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${db_name}.mv_src VALUES (1)"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${db_name}.mv_src ORDER BY i"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${db_name}.mv ORDER BY i"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${db_name}.mv_target ORDER BY i"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${db_name}.mv"

${CLICKHOUSE_CURL} -sS "$url" -d "CREATE MATERIALIZED VIEW {mv_db_name: Identifier}.{mv_name: Identifier} TO {mv_db_name: Identifier}.{target: Identifier}  AS SELECT * FROM {mv_db_name: Identifier}.{src: Identifier};"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db_name}"
