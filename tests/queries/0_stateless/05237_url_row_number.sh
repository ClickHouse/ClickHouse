#!/usr/bin/env bash
# Tags: no-fasttest
# ^ the Parquet format is not included in the fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A format only attaches `ChunkInfoRowNumbers` to its chunks when the reading step asked for it, so
# every step over a file-like storage has to say whether a row-dependent virtual column was
# requested. `url()` is easy to forget next to `file()` and object storage, and forgetting it does
# not fail - `_row_number` silently comes back as `NULL`.

$CLICKHOUSE_CLIENT -q "
    SELECT _row_number, n
    FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(10)+FORMAT+Parquet', Parquet)
    ORDER BY _row_number
    SETTINGS input_format_parquet_preserve_order = 1"

echo '-- the row numbers a filter leaves behind are the ones of the surviving rows'
$CLICKHOUSE_CLIENT -q "
    SELECT _row_number, n
    FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(10)+FORMAT+Parquet', Parquet)
    WHERE n % 3 = 1
    ORDER BY _row_number
    SETTINGS input_format_parquet_preserve_order = 1"

echo '-- _row_number alone, with no column of the file projected'
$CLICKHOUSE_CLIENT -q "
    SELECT sum(_row_number)
    FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(10)+FORMAT+Parquet', Parquet)"
