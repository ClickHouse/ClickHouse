#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_DIR=$USER_FILES_PATH/$CLICKHOUSE_TEST_UNIQUE_NAME
mkdir -p "$DATA_DIR"
cp -r "$CURDIR"/data_hive/ "$DATA_DIR"

# A PREWHERE that reads no file column leaves the WHERE column (a hive partition column, so also not
# a file column) in the reader's output block past `sample_block`. Both queries must return the row
# count, not abort the server.
$CLICKHOUSE_CLIENT -q """
set use_hive_partitioning = 1;

SELECT count() FROM file('$DATA_DIR/data_hive/partitioning/column0=Elizabeth/sample.parquet')
PREWHERE 1 WHERE column0 = 'Elizabeth';

SELECT count() FROM file('$DATA_DIR/data_hive/partitioning/column0=*/sample.parquet')
PREWHERE 2 WHERE column0 = 'Elizabeth' SETTINGS input_format_parquet_use_native_reader_v3 = 1;
"""

rm -rf "$DATA_DIR"
