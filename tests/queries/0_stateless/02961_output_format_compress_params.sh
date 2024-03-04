#!/usr/bin/env bash
# Tags: replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

file_with_random_postfix=test_02961_`date +%s%6N`.csv

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${file_with_random_postfix}', 'CSV', 'x UInt64', 'zstd') SELECT number FROM numbers(1000000) SETTINGS output_format_compression_level = 10, output_format_compression_zstd_window_log = 30, engine_file_truncate_on_insert = 1;"
# Simple check that output_format_compression_zstd_window_log = 30 works
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${file_with_random_postfix}', 'CSV', 'x UInt64', 'zstd') SETTINGS zstd_window_log_max = 29;" 2>&1 | head -n 1 | grep -c "ZSTD_DECODER_FAILED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${file_with_random_postfix}', 'CSV', 'x UInt64', 'zstd') SETTINGS zstd_window_log_max = 30;"
