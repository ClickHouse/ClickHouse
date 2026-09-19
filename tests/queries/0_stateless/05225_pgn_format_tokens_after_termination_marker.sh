#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: the game termination marker ends the movetext of a `PGN` game, so a move (or any
# other token) that follows it is malformed input and must be rejected instead of being appended to
# `moves`. Comments, variations and escape lines after the marker are still allowed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Test 1: Moves after the game termination marker are an error"
$CLICKHOUSE_LOCAL -q "SELECT moves FROM file('$CUR_DIR/data_pgn/moves_after_marker.pgn', PGN, 'moves String')" 2>&1 \
    | grep -oF "Invalid PGN: unexpected token '2.Nf3' after the game termination marker '1-0'"

echo "Test 2: A repeated marker is an error as well, even when it agrees with the result"
$CLICKHOUSE_LOCAL -q "SELECT moves FROM file('$CUR_DIR/data_pgn/repeated_marker.pgn', PGN, 'moves String')" 2>&1 \
    | grep -oF "Invalid PGN: unexpected token '0-1' after the game termination marker '0-1'"

echo "Test 3: Comments, a variation and an escape line after the marker are fine, and the next game is read"
$CLICKHOUSE_LOCAL -q "SELECT event, result, moves FROM file('$CUR_DIR/data_pgn/trailing_comments_after_marker.pgn', PGN, 'event String, result String, moves String')"
