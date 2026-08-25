#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test for bug fix: Parquet reader crashed when min_bytes_for_seek=0
# The bug occurred because the prefetch loop condition failed when min_bytes_for_seek=0,
# causing record_batch_reader to be nullptr.

$CLICKHOUSE_LOCAL -q "insert into function file('$CLICKHOUSE_TMP/03668_test.parquet') select 1 as x settings engine_file_truncate_on_insert=1;"
$CLICKHOUSE_LOCAL -q "select * from file('$CLICKHOUSE_TMP/03668_test.parquet') settings input_format_parquet_local_file_min_bytes_for_seek=0, input_format_parquet_use_native_reader_v3=0, max_parsing_threads=1;"

