#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for a LOGICAL_ERROR exception in the Parquet V3 native reader:
#   "Unexpected number of rows in column subchunk 0 1"
#
# The bug was an inverted memchr check in Reader.cpp that searched for byte 0
# (non-null marker) instead of byte 1 (null marker) in the null_map. When ALL
# values in a filtered subchunk were NULL and the output column was non-nullable
# with null_as_default disabled, the check incorrectly cleared the null_map
# instead of throwing CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN. This left the
# column with 0 rows while rows_pass was 1, causing the assertion failure.
#
# The bug is specific to the V3 native reader, which applies filter pushdown
# during reading.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
DATA_FILE_NO_NULLS="${CLICKHOUSE_TEST_UNIQUE_NAME}_no_nulls.parquet"

# Create a Parquet file with a Nullable(String) column containing NULLs.
# Every row where id % 3 == 0 has val = NULL.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$DATA_FILE', Parquet)
    SELECT number AS id, if(number % 3 = 0, NULL, toString(number)) AS val
    FROM numbers(20)
"

# Create a second Parquet file with a Nullable(String) column but NO NULLs.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$DATA_FILE_NO_NULLS', Parquet)
    SELECT number AS id, toString(number) AS val
    FROM numbers(20)
"

# 1) Reading with null_as_default=1 (default) — NULLs become empty strings.
echo "--- null_as_default=1 non-nullable ---"
$CLICKHOUSE_CLIENT -q "
    SELECT id, val FROM file('$DATA_FILE', Parquet, 'id UInt64, val String')
    WHERE id IN (0, 3, 6) ORDER BY id
    SETTINGS input_format_null_as_default = 1
"

# 2) Reading nullable output — NULLs preserved.
echo "--- nullable output ---"
$CLICKHOUSE_CLIENT -q "
    SELECT id, val FROM file('$DATA_FILE', Parquet, 'id UInt64, val Nullable(String)')
    WHERE id IN (0, 3, 6) ORDER BY id
"

# 3) Non-nullable with null_as_default=0 and a filter that hits only NULL rows:
#    should throw CANNOT_INSERT_NULL, NOT an assertion failure with LOGICAL_ERROR.
#    Use head -1 because CI's --send_logs_level=warning can cause the error code
#    to appear in both a server log line and the exception message.
echo "--- non-nullable null_as_default=0 should error ---"
$CLICKHOUSE_CLIENT -q "
    SELECT id, val FROM file('$DATA_FILE', Parquet, 'id UInt64, val String')
    WHERE id = 0
    SETTINGS input_format_null_as_default = 0
" 2>&1 | grep -o 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' | head -1

# 4) Non-nullable with null_as_default=0 reading from a file with NO nulls — should succeed.
#    (Tests that the fix doesn't break the non-null case.)
echo "--- non-nullable null_as_default=0 no nulls in file ---"
$CLICKHOUSE_CLIENT -q "
    SELECT id, val FROM file('$DATA_FILE_NO_NULLS', Parquet, 'id UInt64, val String')
    WHERE id IN (1, 2, 4) ORDER BY id
    SETTINGS input_format_null_as_default = 0
"
