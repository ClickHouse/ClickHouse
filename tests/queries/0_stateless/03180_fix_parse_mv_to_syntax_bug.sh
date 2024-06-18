#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&param_target=mv_target&param_src=mv_src&param_mv_name=mv&param_mv_db_name=test_parse_mv_to_syntax_db"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS test_parse_mv_to_syntax_db"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE test_parse_mv_to_syntax_db"

${CLICKHOUSE_CURL} -sS "$url" -d "CREATE OR REPLACE TABLE {mv_db_name: Identifier}.{target: Identifier} (i Int32) ENGINE MergeTree ORDER BY i;"
${CLICKHOUSE_CURL} -sS "$url" -d "CREATE OR REPLACE TABLE {mv_db_name: Identifier}.{src: Identifier} (i Int32) ENGINE MergeTree ORDER BY i;"
${CLICKHOUSE_CURL} -sS "$url" -d "CREATE MATERIALIZED VIEW {mv_db_name: Identifier}.{mv_name: Identifier} TO {mv_db_name: Identifier}.{target: Identifier}  AS SELECT * FROM {mv_db_name: Identifier}.{src: Identifier};"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_parse_mv_to_syntax_db.mv_src VALUES (1)"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM test_parse_mv_to_syntax_db.mv_src ORDER BY i"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM test_parse_mv_to_syntax_db.mv ORDER BY i"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM test_parse_mv_to_syntax_db.mv_target ORDER BY i"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_parse_mv_to_syntax_db.mv"

${CLICKHOUSE_CURL} -sS "$url" -d "CREATE MATERIALIZED VIEW {mv_db_name: Identifier}.{mv_name: Identifier} TO {mv_db_name: Identifier}.{target: Identifier}  AS SELECT * FROM {mv_db_name: Identifier}.{src: Identifier};"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS test_parse_mv_to_syntax_db"
