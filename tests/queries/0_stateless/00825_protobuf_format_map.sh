#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS map_protobuf_00825;

-- Pin Map serialization to 'basic' so the test output (key order in SELECT *
-- and the binary representation in the Protobuf hexdump) is deterministic.
-- CI randomizes map_serialization_version and
-- map_serialization_version_for_zero_level_parts between 'basic' and
-- 'with_buckets'; the latter stores Map entries in hash-bucket order on disk,
-- which both reorders the keys returned by SELECT and changes the bytes in the
-- Protobuf output. The test verifies the Protobuf round-trip for Map, which
-- is orthogonal to the storage layout.
CREATE TABLE map_protobuf_00825
(
  a Map(String, UInt32)
) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';

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
$CLICKHOUSE_CLIENT --query "INSERT INTO map_protobuf_00825 SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_map:Message' FORMAT Protobuf" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM map_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE map_protobuf_00825"
