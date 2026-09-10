#!/usr/bin/env bash
# Test: exercises `formatRow('XML', ...)` over HTTP with `http_write_exception_in_output_format=1`
# Covers: src/Functions/formatRow.cpp:50 — `format_settings.xml.valid_output_on_exception = false;`
# The PR test only covers the `JSONEachRow` branch (line 49). Without this fix line, the XML
# `RowOutputFormatWithExceptionHandlerAdaptor` would create a `peekable_out` PeekableWriteBuffer
# that conflicts with formatRow's per-row `finalize()` + `resetFormatter()` cycle, causing a crash.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Run with http_write_exception_in_output_format=1 — this is the trigger that, before the fix,
# made `getFormatSettings(context)` return `xml.valid_output_on_exception=true` for HTTP queries.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_write_exception_in_output_format=1" \
    --data-binary "SELECT formatRow('XML', number) FROM numbers(3)" | wc -l
