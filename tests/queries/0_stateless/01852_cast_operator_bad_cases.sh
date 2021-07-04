#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT [1,]::Array(UInt8)"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [1, 2]]::Array(UInt8)"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [[1, 2]::Array(UInt8)"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [[1, 2],, []]::Array(Array(UInt8))"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [[1, 2][]]::Array(Array(UInt8))"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [1,,2]::Array(UInt8)"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT [1 2]::Array(UInt8)"  2>&1 | grep -o -m1 'Syntax error'

$CLICKHOUSE_CLIENT --query="SELECT 1 4::UInt32"  2>&1 | grep -o 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT '1' '4'::UInt32"  2>&1 | grep -o -m1 'Syntax error'
$CLICKHOUSE_CLIENT --query="SELECT '1''4'::UInt32"  2>&1 | grep -o -m1 'Code: 6'
