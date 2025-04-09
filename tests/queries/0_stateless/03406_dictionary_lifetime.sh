#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP DICTIONARY IF EXISTS dx;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS tbl;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE tbl (col Int) ENGINE = Memory;"

# MIN negative value
$CLICKHOUSE_CLIENT --query="
CREATE DICTIONARY dx (
    col Int DEFAULT 1
)
PRIMARY KEY (col)
SOURCE(CLICKHOUSE(TABLE 'tbl'))
LAYOUT(HASHED_ARRAY())
LIFETIME(MIN -1 MAX 0)
" 2>&1 | grep -q "Code: 62. DB::Exception: Syntax error: failed at position 142 ()) (line 7, col 22): )
;. Expected one of: nothing, key-value pair, identifier. (SYNTAX_ERROR)" && echo 'OK' || echo 'FAIL'

# MAX negative value
$CLICKHOUSE_CLIENT --query="
CREATE DICTIONARY dx (
    col Int DEFAULT 1
)
PRIMARY KEY (col)
SOURCE(CLICKHOUSE(TABLE 'tbl'))
LAYOUT(HASHED_ARRAY())
LIFETIME(MIN 0 MAX -1)
" 2>&1 | grep -q "Code: 62. DB::Exception: Syntax error: failed at position 142 ()) (line 7, col 22): )
;. Expected one of: nothing, key-value pair, identifier. (SYNTAX_ERROR)" && echo 'OK' || echo 'FAIL'

# MIN > MAX
$CLICKHOUSE_CLIENT --query="
CREATE DICTIONARY dx (
    col Int DEFAULT 1
)
PRIMARY KEY (col)
SOURCE(CLICKHOUSE(TABLE 'tbl'))
LAYOUT(HASHED_ARRAY())
LIFETIME(MIN 1 MAX 0)
" 2>&1 | grep -q "DB::Exception: Dictionary lifetime parameter 'MIN' must be less than or equal to 'MAX'." && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="DROP DICTIONARY IF EXISTS dx;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS tbl;"
