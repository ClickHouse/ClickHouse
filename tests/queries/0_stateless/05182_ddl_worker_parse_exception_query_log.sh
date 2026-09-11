#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

UDF="ddl_worker_parse_exception_05182_${CLICKHOUSE_DATABASE}"
TABLE="ddl_worker_parse_exception_05182"
QUERY_ID="ddl_worker_parse_exception_05182_${CLICKHOUSE_TEST_UNIQUE_NAME}"
QUERY_SIZE_LIMIT=512
PARSER_DEPTH_LIMIT=50

deep_expression="x"
for _ in {1..100}; do
    deep_expression="array(${deep_expression})"
done

create_function_query="CREATE FUNCTION ${UDF} AS x -> ${deep_expression}"
ddl_query="CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost ENGINE = Memory AS SELECT ${UDF}(number) AS value FROM numbers(1)"

if (( ${#ddl_query} >= QUERY_SIZE_LIMIT )); then
    echo "The initial DDL query must be shorter than max_query_size" >&2
    exit 1
fi

if (( ${#create_function_query} <= QUERY_SIZE_LIMIT )); then
    echo "The rewritten DDL query must be longer than max_query_size" >&2
    exit 1
fi

cleanup()
{
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
        --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${UDF}" >/dev/null 2>&1 || true
}

cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "${create_function_query}"

if worker_error=$(${CLICKHOUSE_CLIENT} \
    --query_id="${QUERY_ID}" \
    --max_query_size=${QUERY_SIZE_LIMIT} \
    --max_parser_depth=${PARSER_DEPTH_LIMIT} \
    --distributed_ddl_output_mode=throw \
    --query "${ddl_query}" 2>&1); then
    echo "The worker-side parse failure was expected" >&2
    exit 1
fi

grep -qF "TOO_DEEP_RECURSION" <<< "${worker_error}"
echo "worker parse limit enforced"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 AND countIf(notEmpty(query) AND position(query, 'CREATE TABLE') > 0) = count()
    FROM system.query_log
    WHERE type = 'ExceptionBeforeStart'
        AND initial_query_id = '${QUERY_ID}'
        AND position(exception, 'TOO_DEEP_RECURSION') > 0
"
