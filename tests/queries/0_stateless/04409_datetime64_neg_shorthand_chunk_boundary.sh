#!/usr/bin/env bash
# Regression: the bare `-.123` shorthand for DateTime64 container/JSON elements must parse
# correctly even when the '-' and '.' land in different read-buffer chunks. Piping the input
# through stdin (a streaming ReadBuffer that refills at the buffer boundary) with
# input_format_parallel_parsing = 0 and max_read_buffer_size = 1 makes every byte its own
# refill, so '-' is the last byte of one chunk and '.'/the digit is the first byte of the
# next. A lookahead that only inspects the current chunk would miss the '.' and fail with
# CANNOT_PARSE_NUMBER; the parse must succeed and a lone '-' must still be rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts="input_format_parallel_parsing = 0, max_read_buffer_size = 1, session_timezone = 'UTC'"

printf '{"x":[-.123]}\n{"x":[-.877,-1.5]}\n{"x":[.123,-0.123]}\n' \
    | ${CLICKHOUSE_LOCAL} --input-format=JSONEachRow --structure="x Array(DateTime64(3, 'UTC'))" \
        --query "SELECT 'json', arrayMap(e -> toString(e), x) FROM table SETTINGS $opts"

printf '"[-.123,-0.877,.5]"\n' \
    | ${CLICKHOUSE_LOCAL} --input-format=CSV --structure="x Array(DateTime64(3, 'UTC'))" \
        --query "SELECT 'csv', arrayMap(e -> toString(e), x) FROM table SETTINGS $opts"

# A lone '-' (no fraction, no magnitude) must still be rejected, not silently parsed as 0.
printf '{"x":[-]}\n' \
    | ${CLICKHOUSE_LOCAL} --input-format=JSONEachRow --structure="x Array(DateTime64(3, 'UTC'))" \
        --query "SELECT * FROM table SETTINGS $opts" 2>&1 \
    | grep -c -m1 CANNOT_PARSE_NUMBER
