#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format is not available in fasttest builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Errors produced by the Arrow library itself (as opposed to exceptions raised
# by ClickHouse code called from it, see 04335_arrow_memory_limit_error_code)
# must keep being reported with the per-format error codes. Truncated files
# make the library fail on its own while parsing.

DATA_PREFIX=$CLICKHOUSE_TMP/$CLICKHOUSE_DATABASE

for format in Arrow ArrowStream ORC; do
    file=$DATA_PREFIX.$format
    $CLICKHOUSE_LOCAL -q "SELECT number AS x, repeat('a', 100) AS s FROM numbers(1000) FORMAT $format" > "$file"
    head -c $(( $(stat -c%s "$file") / 2 )) "$file" > "$file.truncated"
done

# The structure is specified explicitly to avoid schema inference, which wraps
# all errors into CANNOT_EXTRACT_TABLE_STRUCTURE.

# Truncation cuts the record batch body mid-stream.
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_PREFIX.ArrowStream.truncated', ArrowStream, 'x UInt64, s String')" 2>&1 \
    | grep -o -m1 'CANNOT_READ_ALL_DATA'
# The Arrow file format keeps the footer at the end, so truncation breaks opening.
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_PREFIX.Arrow.truncated', Arrow, 'x UInt64, s String')" 2>&1 \
    | grep -o -m1 'UNKNOWN_EXCEPTION'
# The legacy Arrow-based ORC reader (the fast decoder is a separate code path).
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_PREFIX.ORC.truncated', ORC, 'x UInt64, s String') SETTINGS input_format_orc_use_fast_decoder = 0" 2>&1 \
    | grep -o -m1 'BAD_ARGUMENTS'

rm -f "$DATA_PREFIX".{Arrow,ArrowStream,ORC}{,.truncated}
