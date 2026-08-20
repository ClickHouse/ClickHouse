#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for heap-use-after-free in Parquet UUID writer column statistics.
#
# Bug: ConverterUUID stored UUIDs in a member PODArray (`swapped_buf`) and `StatisticsFixedStringRef`
# kept raw `const uint8_t *` pointers into that buffer. When successive `getBatch` calls grew the
# buffer past its capacity (e.g. after a low-density null run), the realloc invalidated the
# pointers held by `total_statistics`, and the next `merge` call's `memcmp` read freed memory.
# Detected by AddressSanitizer in BuzzHouse with STID 1288-3e88.
#
# Fix: switch ConverterUUID's Statistics type to `StatisticsFixedStringCopy<16, false>`, which
# stores min/max as inline 16-byte arrays (no dangling pointers).
#
# This test forces multi-page writes with clustered nulls so `data_count` per batch oscillates
# wildly, which previously caused `swapped_buf` to grow and realloc between flushed pages.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# Generate 100k rows of Nullable(UUID) where every other 1000-row run is all-NULL.
# With output_format_parquet_data_page_size = 1024 and the custom encoder,
# this yields many small pages where data_count varies between 0 and ~1000 per batch,
# triggering swapped_buf reallocs. Pre-fix, this is a heap-use-after-free under ASan.
$CLICKHOUSE_LOCAL -q "
    SELECT if(number % 2000 < 1000, NULL, generateUUIDv4()) AS u
    FROM numbers(100000)
    INTO OUTFILE '${DATA_FILE}'
    SETTINGS output_format_parquet_data_page_size = 1024,
             output_format_parquet_batch_size = 1024,
             output_format_parquet_max_dictionary_size = 0,
             output_format_parquet_use_custom_encoder = 1
    FORMAT Parquet;
"

# Sanity: read back. Must round-trip the row count and null count.
$CLICKHOUSE_LOCAL -q "
    SELECT count() AS rows, sum(toUInt8(u IS NULL)) AS nulls
    FROM file('${DATA_FILE}', Parquet, 'u Nullable(UUID)');
"

rm -f "${DATA_FILE}"
