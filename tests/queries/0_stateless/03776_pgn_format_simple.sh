#!/usr/bin/env bash
# Tags: no-fasttest
# Simple test for PGN format

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test 1: Basic select
echo "=== Test 1: Basic select with explicit schema ==="
$CLICKHOUSE_LOCAL -q "SELECT white, black, result FROM file('$CURDIR/data_pgn/simple_games.pgn', PGN, 'white String, black String, result String')" 

# Test 2: Full schema
echo "=== Test 2: Full schema (describe) ==="
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$CURDIR/data_pgn/simple_games.pgn', PGN)"

# Test 3: With ELO ratings
echo "=== Test 3: With ELO ratings ==="
$CLICKHOUSE_LOCAL -q "SELECT white, black, white_elo, black_elo FROM file('$CURDIR/data_pgn/simple_games.pgn', PGN, 'white String, black String, white_elo Int32, black_elo Int32')"

# Test 4: Count games
echo "=== Test 4: Count games ==="
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM file('$CURDIR/data_pgn/simple_games.pgn', PGN, 'white String')"

# Test 5: Filter
echo "=== Test 5: Filter by result ==="
$CLICKHOUSE_LOCAL -q "SELECT white, black, result FROM file('$CURDIR/data_pgn/simple_games.pgn', PGN, 'white String, black String, result String') WHERE result = '1-0'"

# Test 6: Insert into table
echo "=== Test 6: Insert into table ==="
$CLICKHOUSE_LOCAL -q "
CREATE TABLE pgn_test (
    event String,
    white String,
    black String,
    result String,
    white_elo Int32
) ENGINE = Memory;

INSERT INTO pgn_test
SELECT event, white, black, result, white_elo
FROM file('$CURDIR/data_pgn/simple_games.pgn', PGN);

SELECT COUNT(*) FROM pgn_test;
SELECT white, result FROM pgn_test WHERE white_elo > 2550;
"

# Test 7: All columns
echo "=== Test 7: All columns ==="
$CLICKHOUSE_LOCAL -q "SELECT 
    event, site, date, round, white, black, result, white_elo, black_elo, 
    LENGTH(moves) > 0 as has_moves 
FROM file('$CURDIR/data_pgn/simple_games.pgn', PGN)"
