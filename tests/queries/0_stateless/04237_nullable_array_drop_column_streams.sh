#!/usr/bin/env bash
# Tags: no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

run_query()
{
    ${CLICKHOUSE_CLIENT} --allow_experimental_nullable_array_type=1 "$@"
}

run_query --query "DROP TABLE IF EXISTS nullable_array_drop_column"

run_query --query "
CREATE TABLE nullable_array_drop_column
(
    id UInt32,
    arr Nullable(Array(UInt32))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
         enable_block_number_column = 0, enable_block_offset_column = 0,
         replace_long_file_name_to_hash = 0;
"

run_query --query "
INSERT INTO nullable_array_drop_column VALUES
    (1, [1, 2, 3]),
    (2, NULL);
"

PART_PATH=$(run_query --query "
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = 'nullable_array_drop_column'
      AND active
    LIMIT 1
")

echo "Before drop:"
ls "${PART_PATH}" | grep -E '^arr\.' | sed -E 's/\.(bin|mrk2|cmrk2|mrk)$//' | sort -u

run_query --query "
ALTER TABLE nullable_array_drop_column
    DROP COLUMN arr
    SETTINGS mutations_sync = 2;
"

PART_PATH=$(run_query --query "
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = 'nullable_array_drop_column'
      AND active
    LIMIT 1
")

echo "After drop:"
ls "${PART_PATH}" 2>/dev/null | grep -E '^arr\.' | sed -E 's/\.(bin|mrk2|cmrk2|mrk)$//' | sort -u || true

run_query --query "SELECT throwIf(sum(id) != 3) FROM nullable_array_drop_column FORMAT Null"

run_query --query "DROP TABLE nullable_array_drop_column"
