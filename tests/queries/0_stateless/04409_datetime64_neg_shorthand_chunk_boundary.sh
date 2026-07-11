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

# These must be passed as clickhouse-local command-line options, not a query-level SETTINGS
# clause: SETTINGS on the SELECT does not reach the stdin input-format reader, so a CI-randomized
# --date_time_input_format on the command line would otherwise win. In particular the dotted
# -.xxx shorthand is a basic-parser feature (scalar/quoted best_effort reject it), so pin
# date_time_input_format=basic; and max_read_buffer_size=1 + input_format_parallel_parsing=0
# force the byte-per-chunk refill that exercises the '-'/'.' split.
opts=(--input_format_parallel_parsing=0 --max_read_buffer_size=1 --date_time_input_format=basic)

# The timezone is baked into the column type ('UTC') so toString() rendering is
# independent of the CI runner's process timezone (session_timezone alone does not
# override the type's own timezone for arrayMap(toString) here).
printf '{"x":[-.123]}\n{"x":[-.877,-1.5]}\n{"x":[.123,-0.123]}\n' \
    | ${CLICKHOUSE_LOCAL} "${opts[@]}" --input-format=JSONEachRow --structure="x Array(DateTime64(3, 'UTC'))" \
        --query "SELECT 'json', arrayMap(e -> toString(e), x) FROM table"

printf '"[-.123,-0.877,.5]"\n' \
    | ${CLICKHOUSE_LOCAL} "${opts[@]}" --input-format=CSV --structure="x Array(DateTime64(3, 'UTC'))" \
        --query "SELECT 'csv', arrayMap(e -> toString(e), x) FROM table"

# A lone '-' (no fraction, no magnitude) must still be rejected, not silently parsed as 0.
printf '{"x":[-]}\n' \
    | ${CLICKHOUSE_LOCAL} "${opts[@]}" --input-format=JSONEachRow --structure="x Array(DateTime64(3, 'UTC'))" \
        --query "SELECT * FROM table" 2>&1 \
    | grep -c -m1 CANNOT_PARSE_NUMBER
