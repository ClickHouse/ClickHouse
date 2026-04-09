#!/usr/bin/env bash
# Tags: no-fasttest

# Test for https://github.com/ClickHouse/ClickHouse/issues/72596
# ProtobufList format should produce empty output for empty tables,
# not a zero-length message that reads back as a row with default values.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_protobuflist_empty"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_protobuflist_empty_roundtrip"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_protobuflist_empty (c0 Int32) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_protobuflist_empty_roundtrip (c0 Int32) ENGINE = Memory"

# Export from empty table should produce empty output (0 bytes)
BINARY_FILE=$(mktemp)
$CLICKHOUSE_CLIENT --query "SELECT * FROM t_protobuflist_empty FORMAT ProtobufList SETTINGS format_schema = '$SCHEMADIR/03822_protobuflist_empty:Message'" > "$BINARY_FILE"

# Check that output is empty
FILESIZE=$(stat -c%s "$BINARY_FILE" 2>/dev/null || stat -f%z "$BINARY_FILE")
if [ "$FILESIZE" -eq 0 ]; then
    echo "OK: empty table produces empty output"
else
    echo "FAIL: empty table produces $FILESIZE bytes instead of 0"
fi

# Now read the file back via INSERT INTO ... FORMAT and make sure we get 0 rows, not a ghost row
$CLICKHOUSE_CLIENT --throw_if_no_data_to_insert=0 --query "INSERT INTO t_protobuflist_empty_roundtrip SETTINGS format_schema = '$SCHEMADIR/03822_protobuflist_empty:Message' FORMAT ProtobufList" < "$BINARY_FILE"
ROWS=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM t_protobuflist_empty_roundtrip")
if [ "$ROWS" -eq 0 ]; then
    echo "OK: reading empty output gives 0 rows"
else
    echo "FAIL: reading empty output gives $ROWS rows instead of 0"
fi

rm "$BINARY_FILE"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_protobuflist_empty"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_protobuflist_empty_roundtrip"
