#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --param_tbl 'numbers' --query 'select * from system.{tbl:Identifier} limit 1'
$CLICKHOUSE_CLIENT --param_db 'system' --param_tbl 'numbers' --query 'select * from {db:Identifier}.{tbl:Identifier} limit 1'
$CLICKHOUSE_CLIENT --param_col 'number' --query 'select {col:Identifier} from system.numbers limit 1'
$CLICKHOUSE_CLIENT --param_col 'number' --query 'select a.{col:Identifier} from system.numbers a limit 1'
$CLICKHOUSE_CLIENT --param_tbl 'numbers' --param_col 'number' --query 'select sum({tbl:Identifier}.{col:Identifier}) FROM (select * from system.{tbl:Identifier} limit 10) numbers'
