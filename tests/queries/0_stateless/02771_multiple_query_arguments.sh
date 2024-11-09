#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# clickhouse-client
$CLICKHOUSE_CLIENT --query "SELECT 101" --query "SELECT 101"
$CLICKHOUSE_CLIENT --query "SELECT 202;" --query "SELECT 202;"
$CLICKHOUSE_CLIENT --query "SELECT 303" --query "SELECT 303; SELECT 303" 2>&1 | grep -o 'Multi-statements are not allowed'
$CLICKHOUSE_CLIENT --query "" --query "" 2>&1 | grep -o 'Empty query'
$CLICKHOUSE_CLIENT --query "SELECT 303" --query 2>&1 | grep -o 'Bad arguments'
$CLICKHOUSE_CLIENT --query "SELECT 303" --query "SELE" 2>&1 | grep -o 'Syntax error'

# clickhouse-local
$CLICKHOUSE_LOCAL --query "SELECT 101" --query "SELECT 101"
$CLICKHOUSE_LOCAL --query "SELECT 202;" --query "SELECT 202;"
$CLICKHOUSE_LOCAL --query "SELECT 303" --query "SELECT 303; SELECT 303" 2>&1 | grep -o 'Multi-statements are not allowed' # behaves differently than clickhouse-client, TODO make it consistent
$CLICKHOUSE_LOCAL --query "" --query "" 2>&1 | grep -o 'Empty query' # behaves equally different than clickhouse-client TODO
$CLICKHOUSE_LOCAL --query "SELECT 303" --query 2>&1 | grep -o 'Bad arguments'
$CLICKHOUSE_LOCAL --query "SELECT 303" --query "SELE" 2>&1 | grep -o 'Syntax error'
