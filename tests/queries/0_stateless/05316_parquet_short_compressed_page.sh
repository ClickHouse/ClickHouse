#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_parquet"

# A compressed page that decompresses to fewer bytes than its header declares must fail with INCORRECT_DATA
# instead of looping forever. Files from the recipe in #123274: the dictionary page's compressed_page_size
# decremented (ZSTD), and its uncompressed_page_size incremented (Brotli).
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA/05316_parquet_short_page_ok.parquet') WHERE s != ''"
for NAME in zstd_truncated brotli_oversized; do
    timeout -k 10 60 ${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA/05316_parquet_short_page_$NAME.parquet') WHERE s != ''" 2>&1 \
        | grep -o -m1 'INCORRECT_DATA'
done
