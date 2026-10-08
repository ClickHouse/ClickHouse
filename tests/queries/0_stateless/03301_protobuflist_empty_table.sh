#!/usr/bin/env bash
# Tags: no-fasttest

# Test for issue #70059: ProtobufList format assertion error when reading from empty table

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

BINARY_FILE_PATH=$(mktemp "$CURDIR/03301_protobuflist_empty_table.XXXXXX.binary")

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS t0_03301;
DROP TABLE IF EXISTS t1_03301;

CREATE TABLE t0_03301 (c0 Int) ENGINE = Memory();
CREATE TABLE t1_03301 (c0 Int) ENGINE = Memory();
EOF

# First write (empty data)
$CLICKHOUSE_CLIENT --query "SELECT c0 FROM t0_03301 FORMAT ProtobufList SETTINGS format_schema = '$SCHEMADIR/03301_protobuflist_empty_table:Message'" > "$BINARY_FILE_PATH"

# First read (should not crash)
$CLICKHOUSE_CLIENT --throw_if_no_data_to_insert=0 --query "INSERT INTO t0_03301 SETTINGS format_schema='$SCHEMADIR/03301_protobuflist_empty_table:Message' FORMAT ProtobufList" < "$BINARY_FILE_PATH"

# Second write (still empty data)
$CLICKHOUSE_CLIENT --query "SELECT c0 FROM t0_03301 FORMAT ProtobufList SETTINGS format_schema = '$SCHEMADIR/03301_protobuflist_empty_table:Message'" > "$BINARY_FILE_PATH"

# Second read (this used to crash with assertion error)
$CLICKHOUSE_CLIENT --throw_if_no_data_to_insert=0 --query "INSERT INTO t0_03301 SETTINGS format_schema='$SCHEMADIR/03301_protobuflist_empty_table:Message' FORMAT ProtobufList" < "$BINARY_FILE_PATH"

# Now test with actual data
$CLICKHOUSE_CLIENT --query "INSERT INTO t1_03301 VALUES (1), (2), (3)"

# Write actual data
$CLICKHOUSE_CLIENT --query "SELECT c0 FROM t1_03301 FORMAT ProtobufList SETTINGS format_schema = '$SCHEMADIR/03301_protobuflist_empty_table:Message'" > "$BINARY_FILE_PATH"

# Read it back
$CLICKHOUSE_CLIENT --query "INSERT INTO t0_03301 SETTINGS format_schema='$SCHEMADIR/03301_protobuflist_empty_table:Message' FORMAT ProtobufList" < "$BINARY_FILE_PATH"

$CLICKHOUSE_CLIENT --query "SELECT * FROM t0_03301 ORDER BY c0"

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE t0_03301;
DROP TABLE t1_03301;
EOF

rm "$BINARY_FILE_PATH"
