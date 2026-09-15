#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Nullable non-`Bool` columns retain dictionary encoding even when every value is `NULL`, which
# produces an empty gzip-compressed dictionary page. It must round-trip after validating the
# empty member's gzip trailer.
${CLICKHOUSE_LOCAL} -q "
    SELECT CAST(NULL, 'Nullable(String)') AS s FROM numbers(100)
    FORMAT Parquet
    SETTINGS output_format_parquet_compression_method = 'gzip',
        output_format_parquet_max_dictionary_size = 1000000
" | ${CLICKHOUSE_LOCAL} --input-format=Parquet -q "SELECT count(), count(s) FROM table"
