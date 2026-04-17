#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

TEST_DIR_NAME=test_04101_fd_skip
DATA_DIR=${USER_FILES_PATH:?}/$TEST_DIR_NAME

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

# Create a corrupted gzip file
echo "not a real gzip header" > "$DATA_DIR/corrupted.gz"

# With setting enabled: should skip the corrupted file and return 0 rows
$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM file('$DATA_DIR/corrupted.gz', 'LineAsString') SETTINGS engine_file_skip_failed_data_files=1"
