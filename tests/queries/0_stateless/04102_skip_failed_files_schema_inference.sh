#!/usr/bin/env bash
# Tags: no-fasttest
# Test that engine_file_skip_failed_data_files protects data reading but NOT schema inference
# With explicit format: setting works, skips corrupted files
# Without explicit format: schema inference on first file fails, setting does not help

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

TEST_DIR_NAME=test_04102_schema
DATA_DIR=${USER_FILES_PATH:?}/$TEST_DIR_NAME

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

# Create test files: good file, corrupted file, good file
echo "row1" > "$DATA_DIR/good.txt"
gzip -c "$DATA_DIR/good.txt" > "$DATA_DIR/0good.gz"
echo "not a real gzip" > "$DATA_DIR/1broken.gz"
echo "row2" > "$DATA_DIR/good2.txt"
gzip -c "$DATA_DIR/good2.txt" > "$DATA_DIR/2good.gz"
rm "$DATA_DIR/good.txt" "$DATA_DIR/good2.txt"

# Test 1: With explicit format + setting enabled: works! Skips corrupted file, reads good files
echo "=== With explicit format (LineAsString) + setting enabled: SUCCESS ==="
$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM file('$DATA_DIR/**.gz', 'LineAsString') SETTINGS engine_file_skip_failed_data_files=1"

# Test 2: Without format (schema inference needed) + setting enabled: FAILS!
# Schema inference tries to read the first file (corrupted) to detect format, fails before data reading
echo "=== Without format (schema inference) + setting enabled: FAILS ==="
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$DATA_DIR/**.gz') SETTINGS engine_file_skip_failed_data_files=1" 2>&1 | grep -q "INFLATE_FAILED" && echo "1" || echo "0"
