#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --param_tbl 'numbers' --query 'select * from system.{tbl:Identifier} limit 1'
$CLICKHOUSE_CLIENT --param_db 'system' --param_tbl 'numbers' --query 'select * from {db:Identifier}.{tbl:Identifier} limit 1'
$CLICKHOUSE_CLIENT --param_col 'number' --query 'select {col:Identifier} from system.numbers limit 1'
$CLICKHOUSE_CLIENT --param_col 'number' --query 'select a.{col:Identifier} from system.numbers a limit 1'
$CLICKHOUSE_CLIENT --param_tbl 'numbers' --param_col 'number' --query 'select sum({tbl:Identifier}.{col:Identifier}) FROM (select * from system.{tbl:Identifier} limit 10) numbers'
$CLICKHOUSE_CLIENT --param_alias 'numnum' --query 'select number as {alias:Identifier} from system.numbers limit 1 format CSVWithNames'
$CLICKHOUSE_CLIENT --param_alias 'numnum' --param_tbl 'numbers' --query 'select number as {alias:Identifier} from system.{tbl:Identifier} limit 1 format CSVWithNames'

# functions and grouping / ordering support
$CLICKHOUSE_CLIENT --param_alias 'numnum' --query 'select number % 2 as {alias:Identifier}, count() from (select number from system.numbers limit 10) group by {alias:Identifier} order by {alias:Identifier} desc format CSVWithNames'
