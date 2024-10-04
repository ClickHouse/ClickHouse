#!/usr/bin/env bash
# Tags: no-fasttest

# https://github.com/ClickHouse/ClickHouse/issues/31160

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS table_skipped_column_in_nested_00825;

CREATE TABLE table_skipped_column_in_nested_00825 (
    identifier UUID,
    unused1 String,
    modules Nested (
        module_id UInt32,
        supply UInt32,
        temp UInt32
    ),
    modules_nodes Nested (
        opening_time Array(UInt32),
        node_id Array(UInt32),
        closing_time_time Array(UInt32),
        current Array(UInt32),
        coords Nested (
            x Float32,
            y Float64
        )
    )
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO table_skipped_column_in_nested_00825 VALUES ('e4048ead-30a2-45e5-90be-2af1c7137523', 'dummy', [1], [50639], [58114], [[5393]], [[1]], [[3411]], [[17811]], [[(10, 20)]]);

SELECT * FROM table_skipped_column_in_nested_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_skipped_column_in_nested.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM table_skipped_column_in_nested_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_skipped_column_in_nested:UpdateMessage'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_skipped_column_in_nested:UpdateMessage" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO table_skipped_column_in_nested_00825 SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_skipped_column_in_nested:UpdateMessage' FORMAT Protobuf" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM table_skipped_column_in_nested_00825 ORDER BY unused1"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE table_skipped_column_in_nested_00825"
