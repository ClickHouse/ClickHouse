#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tuple-element codecs are children of the element data-type node. Verify the
# JSON round trip and reject an AST that both sets and removes the same codec.
CODEC_JSON=$(${CLICKHOUSE_LOCAL} --enable_tuple_element_codecs 1 -q \
    "SELECT parseQueryToJSON('CREATE TABLE t (x Tuple(a UInt8 CODEC(LZ4), b String)) ENGINE = Memory') FORMAT TSVRaw")
${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --enable_tuple_element_codecs 1 --dialect clickhouse_json -q "$CODEC_JSON" >/dev/null \
    && echo 'codec_json_ok'

CODEC_JSON=${CODEC_JSON/\"name\":\"UInt8\"/\"name\":\"UInt8\",\"remove_codec\":true}
CODEC_OUT=$(${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --enable_tuple_element_codecs 1 --dialect clickhouse_json -q "$CODEC_JSON" 2>&1)
echo "$CODEC_OUT" | grep -om1 'ASTDataType cannot set and remove CODEC at the same time'
