#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_map_type = 1;

DROP TABLE IF EXISTS map_protobuf_00825;

CREATE TABLE map_protobuf_00825
(
  a Map(String, UInt32)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO map_protobuf_00825 VALUES ({'x':5, 'y':7}), ({'z':11}), ({'temp':0}), ({'':0});

SELECT * FROM map_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_map.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM map_protobuf_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_map:Message'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
echo "Binary representation:"
hexdump -C $BINARY_FILE_PATH

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO map_protobuf_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_map:Message'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM map_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE map_protobuf_00825"
