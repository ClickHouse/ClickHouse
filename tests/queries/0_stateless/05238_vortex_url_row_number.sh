#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The same wiring as `05237_url_row_number`, for the Vortex reader: it produces its row index
# column only when the reading step asked for the row numbers, so `url(..., Vortex)` returns
# `_row_number` as `NULL` unless `ReadFromURL` sets `need_row_numbers`.
#
# The response of `?query=` has no length and cannot be seeked, so the reader logs that it is
# loading the whole file into memory - which is the expected way to read a Vortex file from a
# stream, not something this test is about. `send_logs_level` keeps it out of stderr.

$CLICKHOUSE_CLIENT -q "
    SELECT _row_number, n
    FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(10)+FORMAT+Vortex', Vortex)
    ORDER BY _row_number
    SETTINGS input_format_vortex_preserve_order = 1, send_logs_level = 'none'"

echo '-- the row numbers a filter leaves behind are the ones of the surviving rows'
$CLICKHOUSE_CLIENT -q "
    SELECT _row_number, n
    FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(10)+FORMAT+Vortex', Vortex)
    WHERE n % 3 = 1
    ORDER BY _row_number
    SETTINGS input_format_vortex_preserve_order = 1, send_logs_level = 'none'"

echo '-- _row_number alone, with no column of the file projected'
$CLICKHOUSE_CLIENT -q "
    SELECT sum(_row_number)
    FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(10)+FORMAT+Vortex', Vortex)
    SETTINGS send_logs_level = 'none'"
