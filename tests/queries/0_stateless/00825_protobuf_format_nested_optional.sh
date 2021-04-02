#!/usr/bin/env bash

# https://github.com/ClickHouse/ClickHouse/issues/6497

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS nested_optional_protobuf_00825;

CREATE TABLE nested_optional_protobuf_00825
(
  messages Nested
  (
    foo String,
    bar Int64
  )
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nested_optional_protobuf_00825 VALUES (['1'], [0]), (['1', ''], [0, 1]);

SELECT * FROM nested_optional_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_nested_optional.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM nested_optional_protobuf_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_nested_optional:Message'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_nested_optional:Message" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO nested_optional_protobuf_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_nested_optional:Message'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM nested_optional_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE nested_optional_protobuf_00825"
