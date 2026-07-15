#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$WORK_DIR"

# good1.csv and good2.csv contain valid UInt32 values; bad.csv breaks strict CSV parsing under the declared schema.
echo "1" > "$WORK_DIR/good1.csv"
echo "not_a_number" > "$WORK_DIR/bad.csv"
echo "2" > "$WORK_DIR/good2.csv"

# COPYFILE_DISABLE prevents macOS tar from adding AppleDouble `._*` entries.
COPYFILE_DISABLE=1 tar -cf "$WORK_DIR/archive.tar" -C "$WORK_DIR" good1.csv bad.csv good2.csv

# Without the setting: fail-fast on the unparseable entry.
$CLICKHOUSE_CLIENT -q "SELECT sum(a) FROM file('$WORK_DIR/archive.tar :: *.csv', CSV, 'a UInt32')" 2>&1 | grep -q "Exception" && echo "1" || echo "0"

# With the setting enabled: bad.csv is skipped, both good entries inside the same archive are read (sum=3).
# Every skipped file is logged with a warning, so silence server logs.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT sum(a) FROM file('$WORK_DIR/archive.tar :: *.csv', CSV, 'a UInt32') SETTINGS engine_file_skip_failed_data_files=1"

# A corrupted archive container (not a valid tar at all).
echo "this is not a tar archive" > "$WORK_DIR/broken.tar"

# Without the setting: the corrupted container fails the query.
$CLICKHOUSE_CLIENT -q "SELECT sum(a) FROM file('$WORK_DIR/*.tar :: good*.csv', CSV, 'a UInt32')" 2>&1 | grep -q "Exception" && echo "1" || echo "0"

# With the setting enabled: the corrupted container is skipped as a whole, entries from the valid archive are read (sum=3).
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT sum(a) FROM file('$WORK_DIR/*.tar :: good*.csv', CSV, 'a UInt32') SETTINGS engine_file_skip_failed_data_files=1"
