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

# Test 8: Exact moves with compact move-number notation
echo "=== Test 8: Exact moves ==="
$CLICKHOUSE_LOCAL -q "SELECT white, moves FROM file('$CURDIR/data_pgn/simple_games.pgn', PGN, 'white String, moves String')"

# Test 9: Parallel parsing setting should not split games into tag fragments
echo "=== Test 9: Parallel parsing settings ==="
$CLICKHOUSE_LOCAL -q "
SELECT COUNT(), arraySort(groupArray(event))
FROM file('$CURDIR/data_pgn/simple_games.pgn', PGN, 'event String')
SETTINGS max_parsing_threads=4, input_format_parallel_parsing=1, min_chunk_bytes_for_parallel_parsing=1, max_block_size=1"

# Test 10: Unknown target columns should use table DEFAULT expressions
echo "=== Test 10: Target table defaults ==="
$CLICKHOUSE_LOCAL -q "
CREATE TABLE pgn_defaults
(
    event String,
    white String,
    source String DEFAULT 'lichess'
) ENGINE = Memory;

INSERT INTO pgn_defaults FROM INFILE '$CURDIR/data_pgn/simple_games.pgn' FORMAT PGN;

SELECT event, white, source FROM pgn_defaults ORDER BY event;

CREATE TABLE pgn_missing_tag_defaults
(
    event String DEFAULT 'unknown',
    white String
) ENGINE = Memory;

INSERT INTO pgn_missing_tag_defaults FROM INFILE '$CURDIR/data_pgn/edge_cases.pgn' FORMAT PGN;

SELECT countIf(event = 'unknown'), count() FROM pgn_missing_tag_defaults;
"
