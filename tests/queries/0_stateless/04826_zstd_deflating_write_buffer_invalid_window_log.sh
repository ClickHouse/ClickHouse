#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An out-of-bounds `output_format_compression_zstd_window_log` makes the `ZstdDeflatingWriteBuffer`
# constructor throw. It used to leak the just created ZSTD compression context (visible to LeakSanitizer),
# because the destructor of a partially constructed object is not called.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}.csv.zst', 'CSV', 'x UInt64', 'zstd')
    SELECT number FROM numbers(10)
    SETTINGS output_format_compression_zstd_window_log = 1000, engine_file_truncate_on_insert = 1
" 2>&1 | grep -o -m1 'ILLEGAL_CODEC_PARAMETER'
