#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# reuse the test data in 01946_test_zstd_decompression_with_escape_sequence_at_the_end_of_buffer.sh
$CLICKHOUSE_LOCAL --query "SELECT count() FROM file('$CUR_DIR/data_zstd/test_01946.zstd', JSONEachRow, 'foo String') SETTINGS zstd_window_log_max = 20" 2>&1  | grep -c "ZSTD_DECODER_FAILED"
$CLICKHOUSE_LOCAL --query "SELECT count() FROM file('$CUR_DIR/data_zstd/test_01946.zstd', JSONEachRow, 'foo String') SETTINGS zstd_window_log_max = 21"
