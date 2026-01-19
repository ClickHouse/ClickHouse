#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_zlib/03312_compressed.gz

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_tbl_03312;"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_tbl_03312 (value String) ENGINE = MergeTree ORDER BY tuple();"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_tbl_03312 FROM INFILE '${DATA_FILE}' SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0 FORMAT JSONEachRow;"
