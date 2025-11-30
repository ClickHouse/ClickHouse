#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

$CLICKHOUSE_LOCAL <<EOF
DROP TABLE IF EXISTS s_o_s_3447;
CREATE TABLE s_o_s_3447 ENGINE=File('ProtobufSingle', 'nonexist') SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString', input_format_protobuf_oneof_presence=false FORMAT ProtobufSingle;
SELECT 'input_format_protobuf_oneof_presence=false';
DESC s_o_s_3447;
EOF

$CLICKHOUSE_LOCAL <<EOF
DROP TABLE IF EXISTS s_o_s_3447;
CREATE TABLE s_o_s_3447 ENGINE=File('ProtobufSingle', 'nonexist') SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString', input_format_protobuf_oneof_presence=true FORMAT ProtobufSingle;
SELECT 'input_format_protobuf_oneof_presence=true';
DESC s_o_s_3447;
EOF
