#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Left out of `03251_insert_sparse_all_formats` for the same reason, so parsing into the sparse
# serialization is covered here.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_vortex_sparse;
    CREATE TABLE t_vortex_sparse (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "INSERT INTO t_vortex_sparse(a) SELECT number FROM numbers(1000)"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number AS a, 0::UInt64 AS b, '' AS c FROM numbers(1000) FORMAT Vortex" \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_vortex_sparse+FORMAT+Vortex&enable_parsing_to_custom_serialization=1" --data-binary @-

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number AS a FROM numbers(1000) FORMAT Vortex" \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_vortex_sparse(a)+FORMAT+Vortex&enable_parsing_to_custom_serialization=1" --data-binary @-

echo "Hash of all rows:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT sum(sipHash64(*)) FROM t_vortex_sparse"

echo "Sparse columns:"
$CLICKHOUSE_CLIENT -q "
    SELECT column, serialization_kind
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_vortex_sparse' AND active AND column IN ('b', 'c')
    GROUP BY column, serialization_kind
    ORDER BY column, serialization_kind
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_vortex_sparse"
