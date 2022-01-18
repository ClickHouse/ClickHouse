#!/usr/bin/env bash

# https://github.com/ClickHouse/ClickHouse/issues/9069

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS array_of_arrays_protobuf_00825;

CREATE TABLE array_of_arrays_protobuf_00825
(
    a String,
    b Nested (
        c Array(Float64)
    )
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO array_of_arrays_protobuf_00825 VALUES ('one', [[1,2,3],[0.5,0.25],[],[4,5],[0.125,0.0625],[6]]);

SELECT * FROM array_of_arrays_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_array_of_arrays.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM array_of_arrays_protobuf_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_array_of_arrays:AA'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_array_of_arrays:AA" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO array_of_arrays_protobuf_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_array_of_arrays:AA'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM array_of_arrays_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE array_of_arrays_protobuf_00825"
