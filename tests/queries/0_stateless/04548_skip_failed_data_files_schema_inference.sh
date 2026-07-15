#!/usr/bin/env bash
# Tags: no-fasttest
# `engine_file_skip_failed_data_files` protects data reading but NOT schema inference:
# with an explicit format the corrupted file is skipped, while without a format
# schema inference reads the corrupted file and fails regardless of the setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

echo "row1" | gzip -c > "$DATA_DIR/0good.gz"
echo "not a real gzip" > "$DATA_DIR/1broken.gz"
echo "row2" | gzip -c > "$DATA_DIR/2good.gz"

echo "=== With explicit format (LineAsString) + setting enabled: SUCCESS ==="
$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM file('$DATA_DIR/**.gz', 'LineAsString') SETTINGS engine_file_skip_failed_data_files=1"

echo "=== Without format (schema inference) + setting enabled: FAILS ==="
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$DATA_DIR/**.gz') SETTINGS engine_file_skip_failed_data_files=1" 2>&1 | grep -q "INFLATE_FAILED" && echo "1" || echo "0"
