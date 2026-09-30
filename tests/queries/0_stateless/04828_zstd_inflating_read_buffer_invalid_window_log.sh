#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An out-of-bounds `zstd_window_log_max` makes the `ZstdInflatingReadBuffer` constructor throw.
# It used to leak the just created ZSTD decompression context (visible to LeakSanitizer),
# because the destructor of a partially constructed object is not called.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}.csv.zst', 'CSV', 'x UInt64', 'zstd')
    SELECT number FROM numbers(10)
    SETTINGS engine_file_truncate_on_insert = 1
"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.csv.zst', 'CSV', 'x UInt64')
    SETTINGS zstd_window_log_max = 1000
" 2>&1 | grep -o -m1 'ZSTD_DECODER_FAILED'
