#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tuple-element codecs are sparse children of the owning Tuple node. Verify the
# JSON round trip and reject a REMOVE operation that contains a codec child.
CODEC_JSON=$(${CLICKHOUSE_LOCAL} --enable_tuple_element_codecs 1 -q \
    "SELECT parseQueryToJSON('CREATE TABLE t (x Tuple(a UInt8 CODEC(LZ4), b String)) ENGINE = MergeTree ORDER BY tuple()') FORMAT TSVRaw")
${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --enable_tuple_element_codecs 1 --dialect clickhouse_json -q "$CODEC_JSON" >/dev/null \
    && echo 'codec_json_ok'

CODEC_JSON=${CODEC_JSON/\"kind\":\"set\"/\"kind\":\"remove\"}
CODEC_OUT=$(${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --enable_tuple_element_codecs 1 --dialect clickhouse_json -q "$CODEC_JSON" 2>&1)
echo "$CODEC_OUT" | grep -om1 'Tuple element REMOVE CODEC operation cannot contain a codec expression'

CODEC_JSON=${CODEC_JSON/\"kind\":\"remove\"/\"kind\":\"set\"}
CODEC_JSON=${CODEC_JSON/\"element_index\":0/\"element_index\":2}
CODEC_OUT=$(${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --enable_tuple_element_codecs 1 --dialect clickhouse_json -q "$CODEC_JSON" 2>&1)
echo "$CODEC_OUT" | grep -om1 'Tuple CODEC operation index 2 is out of range for 2 elements'
