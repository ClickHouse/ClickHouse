#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS array_3dim_protobuf_00825;

CREATE TABLE array_3dim_protobuf_00825
(
    a_b_c Array(Array(Array(Int32)))
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO array_3dim_protobuf_00825 VALUES ([[], [[]], [[1]], [[2,3],[4]]]), ([[[5, 6, 7]], [[8, 9, 10]]]);

SELECT * FROM array_3dim_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_array_3dim.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM array_3dim_protobuf_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_array_3dim:ABC'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_array_3dim:ABC" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO array_3dim_protobuf_00825 SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_array_3dim:ABC' FORMAT Protobuf" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM array_3dim_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE array_3dim_protobuf_00825"
