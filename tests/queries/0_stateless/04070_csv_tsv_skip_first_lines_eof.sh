#!/usr/bin/env bash

# Verify that skip_first_lines doesn't cause an excessive loop when the file has fewer lines than requested.
# The bug only reproduces with file-based reading (pread), not with inline format() which uses ReadBufferFromMemory.
# On unfixed builds the loop iterates skip_first_lines times even after EOF, so the number of lines to skip
# is chosen to be unreachable: a fixed server stops at the end of the file in constant time, while an unfixed
# one would need centuries. That way the timeout below can be generous, and the test does not depend on how
# fast the machine is - a tight timeout used to fail spuriously when the server was stalled for a few seconds
# by an unrelated hiccup, such as contention inside the sanitizer's allocator in the msan build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SKIP=1000000000000000000

# CSV
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_04070.csv', 'CSV') SELECT number FROM numbers(2) SETTINGS engine_file_truncate_on_insert = 1"
timeout 60 $CLICKHOUSE_CLIENT -q "SELECT * FROM file('${CLICKHOUSE_DATABASE}_04070.csv', 'CSV', 'x UInt64') SETTINGS input_format_csv_skip_first_lines = $SKIP"
echo "CSV: $?"

# TSV
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_04070.tsv', 'TSV') SELECT number FROM numbers(2) SETTINGS engine_file_truncate_on_insert = 1"
timeout 60 $CLICKHOUSE_CLIENT -q "SELECT * FROM file('${CLICKHOUSE_DATABASE}_04070.tsv', 'TSV', 'x UInt64') SETTINGS input_format_tsv_skip_first_lines = $SKIP"
echo "TSV: $?"
