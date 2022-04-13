#!/usr/bin/env bash
# Tags: no-fasttest

# https://github.com/ClickHouse/ClickHouse/issues/7438

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS enum_mapping_protobuf_00825;

CREATE TABLE enum_mapping_protobuf_00825
(
  x Enum16('First'=-100, 'Second'=0, 'Third'=100)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO enum_mapping_protobuf_00825 VALUES ('Second'), ('Third'), ('First'), ('First'), ('Second');

SELECT * FROM enum_mapping_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_enum_mapping.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM enum_mapping_protobuf_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_enum_mapping:EnumMessage'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_enum_mapping:EnumMessage" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO enum_mapping_protobuf_00825 SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_enum_mapping:EnumMessage' FORMAT Protobuf" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM enum_mapping_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE enum_mapping_protobuf_00825"
