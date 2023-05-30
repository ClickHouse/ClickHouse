#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT 101"
$CLICKHOUSE_LOCAL --query "SELECT 202" --query "SELECT 303"
$CLICKHOUSE_LOCAL --query "SELECT 404" --query "SELECT 404" --query "SELECT 404"
$CLICKHOUSE_LOCAL --query --query 2>&1
$CLICKHOUSE_LOCAL --query "" --query "" 2>&1
$CLICKHOUSE_LOCAL --query "SELECT 505;"--query

# Abort if any invalid SQL
$CLICKHOUSE_LOCAL --query "SELECT 606; S" --query "SELECT 606" 2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_LOCAL --query "SELECT 707" --query "SELECT 808; S" 2>&1 | grep -o '201'
$CLICKHOUSE_LOCAL --query "SELECT 909" --query "SELECT 909; S" 2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_LOCAL --query "; SELECT 111;" --query "SELECT 111;" 2>&1 | grep -o 'Empty query'
$CLICKHOUSE_LOCAL --query "SELECT 222;" --query "; SELECT 222;" 2>&1 | grep -o '201'
$CLICKHOUSE_LOCAL --query "SELECT 333;" --query "; SELECT 333;" 2>&1 | grep -o 'Empty query'


$CLICKHOUSE_LOCAL --query --query "SELECT 444;" 2>&1 | grep -o 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL --query "SELECT 555;" --query  2>&1 | grep -o 'Bad arguments'
