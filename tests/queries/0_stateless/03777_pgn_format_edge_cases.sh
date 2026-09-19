#!/usr/bin/env bash
# Tags: no-fasttest
# Edge case tests for PGN format

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test 1: Games with missing optional tags
echo "Test 1: Missing optional tags"
$CLICKHOUSE_LOCAL -q "SELECT white, black, result, event FROM file('$CURDIR/data_pgn/edge_cases.pgn', PGN, 'white String, black String, result String, event String') LIMIT 4"

# Test 2: Multiple games parsing
echo "Test 2: Multiple games count"
$CLICKHOUSE_LOCAL -q "SELECT COUNT(*) FROM file('$CURDIR/data_pgn/edge_cases.pgn', PGN, 'white String')"

# Test 3: All result types
echo "Test 3: Group by result"
$CLICKHOUSE_LOCAL -q "SELECT result, COUNT(*) FROM file('$CURDIR/data_pgn/edge_cases.pgn', PGN, 'result String') GROUP BY result ORDER BY result"

# Test 4: Missing ELO values (should default to 0)
echo "Test 4: Missing ELO values"
$CLICKHOUSE_LOCAL -q "SELECT white, white_elo, black_elo FROM file('$CURDIR/data_pgn/edge_cases.pgn', PGN, 'white String, white_elo Int32, black_elo Int32') LIMIT 2"

# Test 5: Comments, castling, compact tags, and missing Result tag
echo "Test 5: Review regression cases"
$CLICKHOUSE_LOCAL -q "SELECT event, site, result, moves FROM file('$CURDIR/data_pgn/review_cases.pgn', PGN, 'event String, site String, result String, moves String')"

# Test 6: Incompatible requested types should raise an error
echo "Test 6: Incompatible schema"
$CLICKHOUSE_LOCAL -q "SELECT event, white_elo FROM file('$CURDIR/data_pgn/review_cases.pgn', PGN, 'event String, white_elo String')" 2>&1 \
    | grep -oF "Column 'white_elo' must have type Int32 for PGN format"

# Test 7: Malformed PGN should raise an error
echo "Test 7: Malformed PGN"
$CLICKHOUSE_LOCAL -q "SELECT event FROM file('$CURDIR/data_pgn/malformed.pgn', PGN, 'event String')" 2>&1 \
    | grep -oF "Invalid PGN tag"

# Test 8: Unterminated PGN comment should raise an error
echo "Test 8: Unterminated comment"
$CLICKHOUSE_LOCAL -q "SELECT event FROM file('$CURDIR/data_pgn/malformed_comment.pgn', PGN, 'event String')" 2>&1 \
    | grep -oF "Invalid PGN: unterminated comment"

# Test 9: An unknown rating is reported as absent, so a table DEFAULT expression is applied
echo "Test 9: Unknown ELO ratings"
$CLICKHOUSE_LOCAL -q "SELECT white_elo, black_elo FROM file('$CURDIR/data_pgn/unknown_elo.pgn', PGN, 'white_elo Int32, black_elo Int32')"
$CLICKHOUSE_LOCAL -q "
CREATE TABLE test_pgn_unknown_elo (white String, white_elo Int32 DEFAULT 1500) ENGINE = Memory;
INSERT INTO test_pgn_unknown_elo FROM INFILE '$CURDIR/data_pgn/unknown_elo.pgn' FORMAT PGN;
SELECT * FROM test_pgn_unknown_elo;
" < /dev/null

# Test 10: A non-numeric rating is a malformed file
echo "Test 10: Non-numeric ELO rating"
$CLICKHOUSE_LOCAL -q "SELECT white_elo FROM file('$CURDIR/data_pgn/bad_elo.pgn', PGN, 'white_elo Int32')" 2>&1 \
    | grep -oF "Invalid PGN: tag 'WhiteElo' has a non-numeric value 'two thousand'"
