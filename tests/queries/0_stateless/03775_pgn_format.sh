#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test 1: Basic schema inference (no explicit format)
echo "Test 1: Schema inference"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String, result String')" 2>&1

# Test 2: Select specific columns
echo "Test 2: Select all columns with full schema"
$CLICKHOUSE_LOCAL -q "SELECT event, white, black, result, white_elo, black_elo FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'event String, white String, black String, result String, white_elo Int32, black_elo Int32')" 2>&1

# Test 3: Select with filtering
echo "Test 3: Filter by result"
$CLICKHOUSE_LOCAL -q "SELECT white, black, result FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String, result String') WHERE result = '1-0'" 2>&1

# Test 4: Count games
echo "Test 4: Count games"
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String, result String')" 2>&1

# Test 5: Describe table structure
echo "Test 5: Describe table"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$CURDIR/data_pgn/test_games.pgn', PGN)" 2>&1

# Test 6: Aggregation query
echo "Test 6: Aggregation - Count by result"
$CLICKHOUSE_LOCAL -q "SELECT result, COUNT(*) as count FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'result String') GROUP BY result" 2>&1

# Test 7: Test with minimal columns (event, white, black only)
echo "Test 7: Minimal columns"
$CLICKHOUSE_LOCAL -q "SELECT event, white, black FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'event String, white String, black String')" 2>&1

# Test 8: Test ELO ratings
echo "Test 8: Filter by ELO rating"
$CLICKHOUSE_LOCAL -q "SELECT white, black, white_elo, black_elo FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String, white_elo Int32, black_elo Int32') WHERE white_elo > 2700" 2>&1

# Test 9: Test site extraction
echo "Test 9: Group by site"
$CLICKHOUSE_LOCAL -q "SELECT site, COUNT(*) as games FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'site String') GROUP BY site" 2>&1

# Test 10: Test round extraction
echo "Test 10: Sort by round"
$CLICKHOUSE_LOCAL -q "SELECT event, round, white, black FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'event String, round String, white String, black String') ORDER BY event, round" 2>&1

# Test 11: Test moves extraction
echo "Test 11: Get moves"
$CLICKHOUSE_LOCAL -q "SELECT white, black, LENGTH(moves) > 0 as has_moves FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String, moves String')" 2>&1

# Test 12: Test date field
echo "Test 12: Extract dates"
$CLICKHOUSE_LOCAL -q "SELECT date, event FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'date String, event String')" 2>&1

# Test 13: Insert into table and query
echo "Test 13: Create table and insert from PGN"
$CLICKHOUSE_LOCAL -q "
CREATE TABLE test_chess_games (
    event String,
    white String,
    black String,
    result String,
    white_elo Int32,
    black_elo Int32
) ENGINE = Memory;

INSERT INTO test_chess_games
SELECT event, white, black, result, white_elo, black_elo
FROM file('$CURDIR/data_pgn/test_games.pgn', PGN);

SELECT COUNT(*) FROM test_chess_games;
SELECT * FROM test_chess_games WHERE result = '1/2-1/2';
" 2>&1

# Test 14: Test partial columns (missing columns should get defaults)
echo "Test 14: Partial column selection"
$CLICKHOUSE_LOCAL -q "SELECT white, black FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String')" 2>&1

# Test 15: Test with aggregation functions
echo "Test 15: Aggregation - Average ELO"
$CLICKHOUSE_LOCAL -q "SELECT
    ROUND(AVG(white_elo), 0) as avg_white_elo,
    ROUND(AVG(black_elo), 0) as avg_black_elo
FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white_elo Int32, black_elo Int32')" 2>&1

# Test 16: Test ORDER BY
echo "Test 16: Order by result"
$CLICKHOUSE_LOCAL -q "SELECT white, black, result FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String, result String') ORDER BY result" 2>&1

# Test 17: Test LIMIT
echo "Test 17: Limit 1"
$CLICKHOUSE_LOCAL -q "SELECT white, black FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String') LIMIT 1" 2>&1

# Test 18: Test with OFFSET
echo "Test 18: Offset"
$CLICKHOUSE_LOCAL -q "SELECT white, black FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String') LIMIT 1 OFFSET 1" 2>&1

# Test 19: Test NULL handling for missing ELO
echo "Test 19: NULL handling for missing/default values"
$CLICKHOUSE_LOCAL -q "SELECT white, black, white_elo FROM file('$CURDIR/data_pgn/test_games.pgn', PGN, 'white String, black String, white_elo Int32')" 2>&1

# Test 20: Verify format is registered
echo "Test 20: Format registered"
$CLICKHOUSE_LOCAL -q "SELECT name FROM system.formats WHERE name = 'PGN'" 2>&1
