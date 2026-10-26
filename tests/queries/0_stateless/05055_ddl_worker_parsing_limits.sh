#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

UDF="ddl_worker_max_query_size_05055_${CLICKHOUSE_DATABASE}"
DEPTH_UDF="ddl_worker_parse_exception_05055_${CLICKHOUSE_DATABASE}"
TABLE="ddl_worker_max_query_size_05055"
DEPTH_TABLE="ddl_worker_parse_exception_05055"
JSON_TABLE="ddl_worker_json_dialect_05055"
QUERY_ID="ddl_worker_parse_exception_05055_${CLICKHOUSE_TEST_UNIQUE_NAME}"
QUERY_LIMIT=1024
QUERY_SIZE_LIMIT=512
PARSER_DEPTH_LIMIT=50

values=$(seq -s, 0 2047)
create_function_query="CREATE FUNCTION ${UDF} AS x -> has([${values}], x)"
ctas_query="CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost ENGINE = Memory AS SELECT ${UDF}(number) AS found FROM numbers(1)"

if (( ${#ctas_query} >= QUERY_LIMIT )); then
    echo "The initial CTAS query must be shorter than max_query_size" >&2
    exit 1
fi

if (( ${#create_function_query} <= QUERY_LIMIT * 4 )); then
    echo "The expanded SQL UDF must be substantially longer than max_query_size" >&2
    exit 1
fi

cleanup()
{
    for table in "${TABLE}" "${DEPTH_TABLE}" "${JSON_TABLE}"; do
        ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
            --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${table} ON CLUSTER test_shard_localhost" >/dev/null 2>&1 || true
    done
    for udf in "${UDF}" "${DEPTH_UDF}"; do
        ${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${udf}" >/dev/null 2>&1 || true
    done
}

cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "${create_function_query}"
${CLICKHOUSE_CLIENT} --max_query_size=${QUERY_LIMIT} --distributed_ddl_output_mode=none --query "${ctas_query}"
echo "expanded UDF"
${CLICKHOUSE_CLIENT} --query "SELECT found FROM ${CLICKHOUSE_DATABASE}.${TABLE}"

json_ddl_query="CREATE TABLE ${CLICKHOUSE_DATABASE}.${JSON_TABLE} ON CLUSTER test_shard_localhost ENGINE = Memory AS SELECT 2 AS value"
json_ddl=$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('${json_ddl_query}') FORMAT TSVRaw")
${CLICKHOUSE_CLIENT} --enable_json_ast_dialect=1 --dialect=clickhouse_json \
    --distributed_ddl_output_mode=none --query "${json_ddl}"
echo "JSON dialect"
${CLICKHOUSE_CLIENT} --query "SELECT value FROM ${CLICKHOUSE_DATABASE}.${JSON_TABLE}"

external_query="SELECT '${values}'"
if external_error=$(${CLICKHOUSE_CLIENT} --max_query_size=${QUERY_LIMIT} --query "${external_query}" 2>&1); then
    echo "An oversized external query unexpectedly succeeded" >&2
    exit 1
fi

grep -qF "Max query size exceeded" <<< "${external_error}"
echo "external max_query_size enforced"

deep_expression="x"
for _ in {1..100}; do
    deep_expression="array(${deep_expression})"
done

create_depth_function_query="CREATE FUNCTION ${DEPTH_UDF} AS x -> ${deep_expression}"
depth_ddl_query="CREATE TABLE ${CLICKHOUSE_DATABASE}.${DEPTH_TABLE} ON CLUSTER test_shard_localhost ENGINE = Memory AS SELECT ${DEPTH_UDF}(number) AS value FROM numbers(1)"

if (( ${#depth_ddl_query} >= QUERY_SIZE_LIMIT )); then
    echo "The initial DDL query must be shorter than max_query_size" >&2
    exit 1
fi

if (( ${#create_depth_function_query} <= QUERY_SIZE_LIMIT )); then
    echo "The rewritten DDL query must be longer than max_query_size" >&2
    exit 1
fi

${CLICKHOUSE_CLIENT} --query "${create_depth_function_query}"

if worker_error=$(${CLICKHOUSE_CLIENT} \
    --query_id="${QUERY_ID}" \
    --max_query_size=${QUERY_SIZE_LIMIT} \
    --max_parser_depth=${PARSER_DEPTH_LIMIT} \
    --distributed_ddl_output_mode=throw \
    --query "${depth_ddl_query}" 2>&1); then
    echo "The worker-side parse failure was expected" >&2
    exit 1
fi

grep -qF "TOO_DEEP_RECURSION" <<< "${worker_error}"
echo "worker parse limit enforced"

# `DDLTaskBase::parseQueryFromEntry` rejects the entry before `executeQuery` is reached.
# Exercise its exception logging with a secondary query, whose text size is also unlimited.
# Query-level settings let the client parse first, then send the lower limits to the server.
secondary_query="SELECT ${deep_expression/x/number} FROM numbers(1)
    SETTINGS max_query_size = ${QUERY_SIZE_LIMIT}, max_parser_depth = ${PARSER_DEPTH_LIMIT}"
if secondary_error=$(${CLICKHOUSE_CLIENT} \
    --query_id="${QUERY_ID}_secondary" \
    --query_kind=secondary_query \
    --query "${secondary_query}" 2>&1); then
    echo "The secondary query parse failure was expected" >&2
    exit 1
fi

grep -qF "TOO_DEEP_AST" <<< "${secondary_error}"

echo "parse exception query log"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 AND countIf(startsWith(query, 'SELECT array(') AND length(query) = ${QUERY_SIZE_LIMIT}) = count()
    FROM system.query_log
    WHERE type = 'ExceptionBeforeStart'
        AND query_id = '${QUERY_ID}_secondary'
        AND current_database = currentDatabase()
        AND position(exception, 'TOO_DEEP_AST') > 0
"
