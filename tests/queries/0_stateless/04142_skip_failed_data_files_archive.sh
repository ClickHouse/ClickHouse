#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v tar &> /dev/null; then echo "tar not found" 1>&2; exit 1; fi

TEST_DIR_NAME=test_04142_archive
WORK_DIR=${USER_FILES_PATH:?}/$TEST_DIR_NAME

cleanup() {
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

# good1.csv and good2.csv contain valid UInt32 values; bad.csv breaks strict CSV parsing under the declared schema.
echo "1" > good1.csv
echo "not_a_number" > bad.csv
echo "2" > good2.csv

tar -cf archive.tar good1.csv bad.csv good2.csv

# Without setting: fail-fast (any non-zero output means an exception was thrown).
$CLICKHOUSE_CLIENT -q "SELECT sum(a) FROM file('$WORK_DIR/archive.tar :: *.csv', CSV, 'a UInt32')" 2>&1 | grep -c -E "Exception|Cannot parse"

# With setting enabled: bad.csv is skipped, both good entries inside the same archive are read (sum=3).
$CLICKHOUSE_CLIENT -q "SELECT sum(a) FROM file('$WORK_DIR/archive.tar :: *.csv', CSV, 'a UInt32') SETTINGS engine_file_skip_failed_data_files=1"
