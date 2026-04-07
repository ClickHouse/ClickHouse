#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS table_default_protobuf_00825;

CREATE TABLE table_default_protobuf_00825
(
  x Int64,
  y Int64 DEFAULT x * x,
  z Int64 DEFAULT x * x * x
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO table_default_protobuf_00825 (x) VALUES (0), (2), (3), (5);
INSERT INTO table_default_protobuf_00825 VALUES (101, 102, 103);

SELECT * FROM table_default_protobuf_00825 ORDER BY x,y,z;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_table_default.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM table_default_protobuf_00825 ORDER BY x,y,z FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_table_default:Message'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_table_default:Message" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO table_default_protobuf_00825 SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_table_default:Message' FORMAT Protobuf" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM table_default_protobuf_00825 ORDER BY x,y,z"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE table_default_protobuf_00825"
