#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for two `PGN` parser gaps:
# 1. An escape line (a `%` in the first column) inside a parenthesized variation must be ignored
#    to the end of the line, so a `)` written there does not close the variation early.
# 2. A game whose move text has no game termination marker and that has no `Result` tag has no
#    result at all, and must be rejected instead of storing an empty `result`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Test 1: An escape line inside a variation is skipped whole, even when it contains ')'"
$CLICKHOUSE_LOCAL -q "SELECT event, result, moves FROM file('$CUR_DIR/data_pgn/variation_escape_line.pgn', PGN, 'event String, result String, moves String')"

echo "Test 2: The same file read one byte at a time, so the escape line starts at a refill boundary"
$CLICKHOUSE_LOCAL -q "SELECT moves FROM file('$CUR_DIR/data_pgn/variation_escape_line.pgn', PGN, 'moves String') SETTINGS storage_file_read_method = 'pread', max_read_buffer_size = 1"

echo "Test 3: A game with moves but neither a 'Result' tag nor a termination marker is an error"
$CLICKHOUSE_LOCAL -q "SELECT result FROM file('$CUR_DIR/data_pgn/no_result.pgn', PGN, 'result String')" 2>&1 \
    | grep -oF "Invalid PGN game: the movetext has no game termination marker and there is no 'Result' tag"

echo "Test 4: A 'Result' tag without a termination marker is fine"
$CLICKHOUSE_LOCAL -q "SELECT result, moves FROM file('$CUR_DIR/data_pgn/result_tag_without_marker.pgn', PGN, 'result String, moves String')"
