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
# codec must be visited now that the codec is an ordinary child of ASTDataType.
${CLICKHOUSE_CLIENT} --enable_tuple_element_codecs=1 --param_level=3 -q "
    CREATE TABLE ${TABLE}
    (
        payload Tuple(value UInt64 CODEC(ZSTD({level:UInt64})), text String)
    )
    ENGINE = MergeTree
    ORDER BY tuple()"

${CLICKHOUSE_CLIENT} -q "
    SELECT position(compression_codec, 'value UInt64 CODEC(ZSTD(3))') > 0
    FROM system.columns
    WHERE database = currentDatabase() AND table = '${TABLE}' AND name = 'payload'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"
trap - EXIT
