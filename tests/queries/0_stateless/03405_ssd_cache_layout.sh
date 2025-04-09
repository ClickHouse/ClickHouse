#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP DICTIONARY IF EXISTS dx;"

# BLOCK_SIZE
$CLICKHOUSE_CLIENT --query="
CREATE DICTIONARY dx
(
    col Int64 default null
)
PRIMARY KEY (col)
SOURCE(NULL())
LAYOUT(SSD_CACHE(BLOCK_SIZE 0))
LIFETIME(1);
" 2>&1 | grep -q "Code: 36. DB::Exception: Dictionary layout parameter value must be greater than 0. (BAD_ARGUMENTS)" && echo 'OK' || echo 'FAIL'

# WRITE_BUFFER_SIZE
$CLICKHOUSE_CLIENT --query="
CREATE DICTIONARY dx
(
    col Int64 default null
)
PRIMARY KEY (col)
SOURCE(NULL())
LAYOUT(SSD_CACHE(WRITE_BUFFER_SIZE 0))
LIFETIME(1);
" 2>&1 | grep -q "Code: 36. DB::Exception: Dictionary layout parameter value must be greater than 0. (BAD_ARGUMENTS)" && echo 'OK' || echo 'FAIL'
