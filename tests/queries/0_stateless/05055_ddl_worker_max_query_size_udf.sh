#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

UDF="ddl_worker_max_query_size_05055_${CLICKHOUSE_DATABASE}"
TABLE="ddl_worker_max_query_size_05055"
QUERY_LIMIT=1024

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
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
        --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${UDF}" >/dev/null 2>&1 || true
}

cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "${create_function_query}"
${CLICKHOUSE_CLIENT} --max_query_size=${QUERY_LIMIT} --distributed_ddl_output_mode=none --query "${ctas_query}"
${CLICKHOUSE_CLIENT} --query "SELECT found FROM ${CLICKHOUSE_DATABASE}.${TABLE}"

external_query="SELECT '${values}'"
if external_error=$(${CLICKHOUSE_CLIENT} --max_query_size=${QUERY_LIMIT} --query "${external_query}" 2>&1); then
    echo "An oversized external query unexpectedly succeeded" >&2
    exit 1
fi

grep -qF "Max query size exceeded" <<< "${external_error}"
echo "external max_query_size enforced"
