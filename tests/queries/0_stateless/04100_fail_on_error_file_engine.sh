#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

TEST_DIR_NAME=test_04100_fail
DATA_DIR=${USER_FILES_PATH:?}/$TEST_DIR_NAME

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

echo "data" > "$DATA_DIR/good.txt"
gzip -c "$DATA_DIR/good.txt" > "$DATA_DIR/0good.gz"
echo "not a real gzip" > "$DATA_DIR/1broken.gz"
gzip -c "$DATA_DIR/good.txt" > "$DATA_DIR/2good.gz"
rm "$DATA_DIR/good.txt"

# Without setting: should throw an exception (default behavior)
# Check for ZLIB_INFLATE_FAILED error
$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM file('$DATA_DIR/**.gz', 'LineAsString')" 2>&1 | grep -c "INFLATE_FAILED"
