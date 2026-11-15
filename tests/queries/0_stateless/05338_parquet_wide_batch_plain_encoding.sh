#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-asan, no-msan, no-tsan, no-ubsan
# The test needs more than 2 GiB of values in a single batch, so it is heavy on memory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_wide_batch_plain.parquet"
trap 'rm -f "$FILE"' EXIT

# A cheaper sibling of `05175_parquet_wide_values_no_32bit_overflow` that is fast enough for the
# debug build. 1024 rows of ~2.1 MB used to reach the writer as one batch, so the single page took
# more than 2 GiB and failed with `Uncompressed page is too big`. Without a dictionary, compression,
# checksums, a bloom filter and the page index, the batch is only copied, not hashed or compared.
$CLICKHOUSE_LOCAL --max_memory_usage 0 \
    --output_format_parquet_max_dictionary_size 0 \
    --output_format_parquet_compression_method none \
    --output_format_parquet_write_checksums 0 \
    --output_format_parquet_write_bloom_filter 0 \
    --output_format_parquet_write_page_index 0 \
    --output_format_parquet_parallel_encoding 0 \
    --query "
    SELECT number AS n, concat(toString(number), repeat('0123456789', 210000)) AS s
    FROM numbers(1024)
    FORMAT Parquet
" > "$FILE"

# Reading the 2 GiB string column back needs more memory than the CI runners allow, so check the
# file metadata and read only the narrow column.
$CLICKHOUSE_LOCAL --query "
    SELECT num_rows, total_uncompressed_size > 2150402986 FROM file('$FILE', ParquetMetadata)
"
$CLICKHOUSE_LOCAL --query "
    SELECT count(), sum(n) FROM file('$FILE', Parquet)
"
