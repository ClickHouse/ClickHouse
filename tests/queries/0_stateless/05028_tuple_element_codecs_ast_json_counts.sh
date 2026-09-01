#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate parser-valid AST JSON first, then corrupt only the vector counts. The
# deserializer must reject the mismatch before reserve() or per-element reads.
CODEC_JSON=$(${CLICKHOUSE_LOCAL} --allow_experimental_tuple_element_codecs 1 -q \
    "SELECT parseQueryToJSON('CREATE TABLE t (x Tuple(a UInt8 CODEC(LZ4), b String)) ENGINE = Memory') FORMAT TSVRaw")
CODEC_JSON=${CODEC_JSON/\"element_codec_count\":2/\"element_codec_count\":3}
CODEC_OUT=$(${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$CODEC_JSON" 2>&1)
echo "$CODEC_OUT" | grep -om1 'ASTTupleDataType has 3 element codecs but 2 element types during AST JSON deserialization'

REMOVAL_JSON=$(${CLICKHOUSE_LOCAL} -q \
    "SELECT parseQueryToJSON('ALTER TABLE t MODIFY COLUMN x Tuple(a UInt8 REMOVE CODEC, b String)') FORMAT TSVRaw")
REMOVAL_JSON=${REMOVAL_JSON/\"element_codec_removal_count\":2/\"element_codec_removal_count\":3}
REMOVAL_OUT=$(${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$REMOVAL_JSON" 2>&1)
echo "$REMOVAL_OUT" | grep -om1 'ASTTupleDataType has 3 element codec removals but 2 element types during AST JSON deserialization'
