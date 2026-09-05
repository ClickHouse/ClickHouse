#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests: a UTF-8 BOM at the beginning of a PGN file and the escape mechanism
# (a line whose first character is `%`) must not produce phantom rows or leak into `moves`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Test 1: A file with a UTF-8 BOM is read without a phantom row"
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM file('$CUR_DIR/data_pgn/bom.pgn', PGN, 'event String')"
$CLICKHOUSE_LOCAL -q "SELECT event, white, black, result, moves FROM file('$CUR_DIR/data_pgn/bom.pgn', PGN, 'event String, white String, black String, result String, moves String')"

echo "Test 2: Escape lines are skipped in all positions"
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM file('$CUR_DIR/data_pgn/escape_lines.pgn', PGN, 'event String')"
$CLICKHOUSE_LOCAL -q "SELECT event, result, moves FROM file('$CUR_DIR/data_pgn/escape_lines.pgn', PGN, 'event String, result String, moves String')"
