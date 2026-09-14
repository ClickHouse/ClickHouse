#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_tuple_codec_ast_children_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"
cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"
}
trap cleanup EXIT

# Query-parameter substitution is a generic AST traversal. The parameter inside the element
# codec must be visited because the sparse operation list is an ordinary Tuple AST child.
${CLICKHOUSE_CLIENT} --enable_tuple_element_codecs=1 --param_variant=bit -q "
    CREATE TABLE ${TABLE}
    (
        payload Tuple(value UInt64 CODEC(T64({variant:String}), LZ4), text String)
    )
    ENGINE = MergeTree
    ORDER BY tuple()"

${CLICKHOUSE_CLIENT} -q "
    SELECT position(create_table_query, 'value UInt64 CODEC(T64(''bit''), LZ4)') > 0
    FROM system.tables
    WHERE database = currentDatabase() AND name = '${TABLE}'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"
trap - EXIT
