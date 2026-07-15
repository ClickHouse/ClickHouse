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

# Without the setting, the query must fail with a decompression error (default behavior).
$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM file('$DATA_DIR/**.gz', 'LineAsString')" 2>&1 | grep -q "INFLATE_FAILED" && echo "1" || echo "0"
