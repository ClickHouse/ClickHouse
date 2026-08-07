#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCHEMADIR="$CUR_DIR/format_schemas"
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('nonexist', 'Protobuf') FORMAT Vertical SETTINGS format_schema='$SCHEMADIR/03252_recursive_type.proto:Struct', input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference=0" |& grep -c BAD_ARGUMENTS
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('nonexist', 'Protobuf') FORMAT Vertical SETTINGS format_schema='$SCHEMADIR/03252_recursive_type.proto:Struct', input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference=1"
