#!/usr/bin/env bash
# Tags: log-engine

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Match the guard's own message, so an earlier generic check throwing the same error code cannot keep this green.
function create_and_match_error()
{
    $CLICKHOUSE_CLIENT --query "CREATE TABLE t_engine_args (a UInt64) ENGINE = $1" 2>&1 | grep -o -m1 -F "$2"
}

create_and_match_error "File()" "Storage File requires from 1 to 3 arguments"
create_and_match_error "File(CSV, 'a.csv', 'none', 'extra')" "Storage File requires from 1 to 3 arguments"
create_and_match_error "Log('x')" "Engine Log doesn't support any arguments (1 given)"
create_and_match_error "TinyLog('x')" "Engine TinyLog doesn't support any arguments (1 given)"
create_and_match_error "StripeLog('x')" "Engine StripeLog doesn't support any arguments (1 given)"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_engine_args'"
