#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

F="${CLICKHOUSE_TMP}/03408_parquet_checksums.parquet"

corrupt_file() {
    # Get column chunk offset and length.
    r=`${CLICKHOUSE_LOCAL} -q "select row_groups[1].file_offset, row_groups[1].total_compressed_size from file('$F', ParquetMetadata)"`

    # Split the two tab-separated tokens.
    IFS=$'\t' read -r offset length <<< "$r"

    # Overwrite the last byte of column chunk (outside page header) with garbage.
    printf '\x42' | dd of="$F" bs=1 seek=$(( offset + length - 1 )) count=1 conv=notrunc status=none
}

# Write file with checksums.
${CLICKHOUSE_LOCAL} -q "
    insert into function file('$F') select 1234567890123456 as x settings engine_file_truncate_on_insert=1, output_format_parquet_compression_method='none', output_format_parquet_write_checksums=1;
    select * from file('$F');"

corrupt_file

${CLICKHOUSE_LOCAL} -q "
    select * from file('$F') settings input_format_parquet_verify_checksums=1
" 2>&1 | grep -o 'CRC checksum verification failed' || echo 'got no checksum error, unexpected'

${CLICKHOUSE_LOCAL} -q "
    select * from file('$F') settings input_format_parquet_verify_checksums=0
" 2>&1 | grep -o 'CRC checksum verification failed' || echo 'no checksum error, as expected'


# Write file without checksums.
${CLICKHOUSE_LOCAL} -q "
    insert into function file('$F') select 1234567890123456 as x settings engine_file_truncate_on_insert=1, output_format_parquet_compression_method='none', output_format_parquet_write_checksums=0;
    select * from file('$F');"

corrupt_file

${CLICKHOUSE_LOCAL} -q "
    select * from file('$F') settings input_format_parquet_verify_checksums=1
" 2>&1 | grep -o 'CRC checksum verification failed' || echo 'no checksum error, as expected'
