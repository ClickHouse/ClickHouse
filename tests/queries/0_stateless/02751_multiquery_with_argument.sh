#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --multiquery "SELECT 100"
$CLICKHOUSE_LOCAL --multiquery "SELECT 101;"
$CLICKHOUSE_LOCAL --multiquery "SELECT 102;SELECT 103;"

# Invalid SQL.
$CLICKHOUSE_LOCAL --multiquery "SELECT 200; S" 2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_LOCAL --multiquery "; SELECT 201;" 2>&1 | grep -o 'Empty query'
$CLICKHOUSE_LOCAL --multiquery "; S; SELECT 202" 2>&1 | grep -o 'Empty query'

# Simultaneously passing --queries-file + --query (multiquery) is prohibited.
$CLICKHOUSE_LOCAL --queries-file "queries.csv" --multiquery "SELECT 250;" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --queries-file "queries.csv" --multiquery "SELECT 251;" 2>&1 | grep -o 'BAD_ARGUMENTS'

# Error expectation cases.                      
# -n <SQL> is also interpreted as a query
$CLICKHOUSE_LOCAL -n "SELECT 301"
$CLICKHOUSE_LOCAL -n "SELECT 302;"
$CLICKHOUSE_LOCAL -n "SELECT 304;SELECT 305;"
# --multiquery or -n is obselete, so we will remove multiquery and -n option except --multiquery <SQL>
# we don't limit the number of times "--multiquery <SQL>" can be used, but we don't recommend it!
# $CLICKHOUSE_LOCAL --multiquery --multiquery
# $CLICKHOUSE_LOCAL -n --multiquery
# $CLICKHOUSE_LOCAL --multiquery -n
$CLICKHOUSE_LOCAL --multiquery --multiquery "SELECT 306; SELECT 307;"
$CLICKHOUSE_LOCAL -n --multiquery "SELECT 307; SELECT 308;"
$CLICKHOUSE_LOCAL --multiquery "SELECT 309; SELECT 310;" --multiquery
$CLICKHOUSE_LOCAL --multiquery "SELECT 311;" --multiquery "SELECT 312;"
$CLICKHOUSE_LOCAL --multiquery "SELECT 313;" -n "SELECT 314;"
$CLICKHOUSE_LOCAL -n "SELECT 320" --query "SELECT 317;"
$CLICKHOUSE_LOCAL --query -n "SELECT 400;" 2>&1 | grep -o 'Bad arguments'
# trans to $CLICKHOUSE_LOCAL --query -q "SELECT 401"
$CLICKHOUSE_LOCAL --query -n --multiquery "SELECT 401;"
