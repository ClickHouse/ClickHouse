#!/usr/bin/env bash

# Verify that skip_first_lines doesn't cause an excessive loop when the file has fewer lines than requested.
# The bug only reproduces with file-based reading (pread), not with inline format() which uses ReadBufferFromMemory.
# On unfixed builds, the loop iterates skip_first_lines times even after EOF, making the query take unreasonably long.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# CSV
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_04070.csv', 'CSV') SELECT number FROM numbers(2) SETTINGS engine_file_truncate_on_insert = 1"
timeout 10 $CLICKHOUSE_CLIENT -q "SELECT * FROM file('${CLICKHOUSE_DATABASE}_04070.csv', 'CSV', 'x UInt64') SETTINGS input_format_csv_skip_first_lines = 1000000000"
echo "CSV: $?"

# TSV
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_04070.tsv', 'TSV') SELECT number FROM numbers(2) SETTINGS engine_file_truncate_on_insert = 1"
timeout 10 $CLICKHOUSE_CLIENT -q "SELECT * FROM file('${CLICKHOUSE_DATABASE}_04070.tsv', 'TSV', 'x UInt64') SETTINGS input_format_tsv_skip_first_lines = 1000000000"
echo "TSV: $?"
