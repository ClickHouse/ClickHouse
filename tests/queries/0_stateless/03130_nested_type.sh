#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCHEMADIR="$CUR_DIR/format_schemas"
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('nonexist', 'Protobuf') SETTINGS format_schema='$SCHEMADIR/03130_nested_schema.proto:Outer'"
