#!/usr/bin/env bash
# Tags: no-fasttest

# https://github.com/ClickHouse/ClickHouse/issues/11117

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS nested_in_nested_protobuf_00825;

CREATE TABLE nested_in_nested_protobuf_00825 (x Nested (y Nested (z Int64))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nested_in_nested_protobuf_00825 VALUES ([[(1),(2)],[(3),(4),(5)]]), ([[(6)]]), ([[]]), ([]);

SELECT * FROM nested_in_nested_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_nested_in_nested.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM nested_in_nested_protobuf_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_nested_in_nested:MessageType'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_nested_in_nested:MessageType" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO nested_in_nested_protobuf_00825 SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_nested_in_nested:MessageType' FORMAT Protobuf" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM nested_in_nested_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE nested_in_nested_protobuf_00825"
