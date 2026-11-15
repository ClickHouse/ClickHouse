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

echo "data" | gzip -c > "$DATA_DIR/0good.gz"
echo "not a real gzip" > "$DATA_DIR/1broken.gz"
echo "data" | gzip -c > "$DATA_DIR/2good.gz"

# With the setting enabled, the broken file is skipped and data from the 2 good files is returned.
# Every skipped file is logged with a warning, so silence server logs.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT count(*) FROM file('$DATA_DIR/**.gz', 'LineAsString') SETTINGS engine_file_skip_failed_data_files=1"
