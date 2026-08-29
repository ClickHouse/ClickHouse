#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests: the `result` of a game is carried both by the `Result` tag and by the game
# termination marker of the move text. A tag value outside `1-0`, `0-1`, `1/2-1/2`, `*` and a
# termination marker that contradicts the tag must both raise an error instead of storing bad data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Test 1: An invalid 'Result' tag value is an error"
$CLICKHOUSE_LOCAL -q "SELECT result FROM file('$CUR_DIR/data_pgn/bad_result_tag.pgn', PGN, 'result String')" 2>&1 \
    | grep -oF "Invalid PGN: tag 'Result' has value 'won', expected '1-0', '0-1', '1/2-1/2' or '*'"

echo "Test 2: A game termination marker that contradicts the 'Result' tag is an error"
$CLICKHOUSE_LOCAL -q "SELECT result FROM file('$CUR_DIR/data_pgn/contradicting_result.pgn', PGN, 'result String')" 2>&1 \
    | grep -oF "Invalid PGN: the game termination marker '0-1' contradicts the game result '1-0'"

echo "Test 3: A game termination marker that agrees with the 'Result' tag is fine"
$CLICKHOUSE_LOCAL -q "SELECT event, result, moves FROM file('$CUR_DIR/data_pgn/simple_games.pgn', PGN, 'event String, result String, moves String') LIMIT 1"
