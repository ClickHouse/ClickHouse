#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

$CLICKHOUSE_LOCAL <<EOF
DROP TABLE IF EXISTS record_04046;
CREATE TABLE record_04046 ENGINE=File('ProtobufSingle', 'nonexist') SETTINGS format_schema='$SCHEMADIR/04046_record.proto:Record', input_format_protobuf_oneof_presence=true FORMAT ProtobufSingle;
DESC record_04046;
DROP TABLE record_04046;
EOF

$CLICKHOUSE_LOCAL <<EOF
DROP TABLE IF EXISTS record_empty_04046;
CREATE TABLE record_empty_04046 ENGINE=File('ProtobufSingle', 'nonexist') SETTINGS format_schema='$SCHEMADIR/04046_empty_record.proto:Record', input_format_protobuf_oneof_presence=true FORMAT ProtobufSingle;
DESC record_empty_04046;
DROP TABLE record_empty_04046;
EOF
