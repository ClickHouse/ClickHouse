#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for a server crash (SIGSEGV) in ProtobufRowInputFormat when a valid
# message precedes a bad (skippable) message in the same block, with
# input_format_allow_errors_num > 0. After a parse error the serializer is destroyed and
# recreated on the next row; before the fix setColumns was called only when the block was
# empty (row_num == 0), so the recreated serializer kept null column pointers and
# dereferenced them in ProtobufSerializer::readRow.
# See https://github.com/ClickHouse/ClickHouse/issues/107644

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CUR_DIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eo pipefail

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS protobuf_skip_bad_row_after_valid"
$CLICKHOUSE_CLIENT --query "CREATE TABLE protobuf_skip_bad_row_after_valid (d Date) ENGINE = MergeTree ORDER BY d"

BINARY_FILE_PATH=$(mktemp "$CLICKHOUSE_TMP/04409_protobuf_skip_bad_row_after_valid.XXXXXX.binary")
trap 'rm -f "$BINARY_FILE_PATH"' EXIT

# Length-delimited Protobuf stream of three messages for `message Row { string d = 1; }`.
# Each message is prefixed by its length; field 1 has wire type LENGTH_DELIMITED (tag 0x0a).
#   1) valid date "2024-01-15"
#   2) bad value "bad!" (skippable: CANNOT_PARSE_DATE)
#   3) valid date "2025-06-16"
{
    printf '\x0c\x0a\x0a'; printf '2024-01-15'
    printf '\x06\x0a\x04'; printf 'bad!'
    printf '\x0c\x0a\x0a'; printf '2025-06-16'
} > "$BINARY_FILE_PATH"

# Must not crash: the bad message is skipped and both valid rows are inserted.
$CLICKHOUSE_CLIENT --input_format_allow_errors_num 10 --query "INSERT INTO protobuf_skip_bad_row_after_valid SETTINGS format_schema = '$SCHEMADIR/04409_protobuf_skip_bad_row_after_valid:Row' FORMAT Protobuf" < "$BINARY_FILE_PATH"

$CLICKHOUSE_CLIENT --query "SELECT d FROM protobuf_skip_bad_row_after_valid ORDER BY d"

$CLICKHOUSE_CLIENT --query "DROP TABLE protobuf_skip_bad_row_after_valid"
