#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

# Create a corrupted gzip file.
echo "not a real gzip header" > "$DATA_DIR/corrupted.gz"

# With the setting enabled, the corrupted file is skipped and 0 rows are returned.
$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM file('$DATA_DIR/corrupted.gz', 'LineAsString') SETTINGS engine_file_skip_failed_data_files=1"
