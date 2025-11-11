#!/usr/bin/env bash
# Tags: no-parallel
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "drop table if exists ref_table"
$CLICKHOUSE_CLIENT --query "drop table if exists alias_table"

$CLICKHOUSE_CLIENT --query "create table ref_table (id UInt32, name String) Engine=MergeTree order by id"
$CLICKHOUSE_CLIENT --query "create table alias_table Engine=Alias($CLICKHOUSE_DATABASE.ref_table)"

$CLICKHOUSE_CLIENT --query "select '-- Insert into reference table --'"
$CLICKHOUSE_CLIENT --query "insert into ref_table values (1, 'one'), (2, 'two'), (3, 'three')"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id"

$CLICKHOUSE_CLIENT --query "select '-- Insert into alias table --'"
$CLICKHOUSE_CLIENT --query "insert into alias_table values (4, 'four')"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id"
$CLICKHOUSE_CLIENT --query "select * from ref_table order by id"
$CLICKHOUSE_CLIENT --query --enable_analyzer=0 "select * from alias_table order by id"

$CLICKHOUSE_CLIENT --query "select '-- Rename reference table --'"
$CLICKHOUSE_CLIENT --query "rename table ref_table to ref_table2"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id" 2>&1 | grep -o -F "Code: 60" | uniq

$CLICKHOUSE_CLIENT --query "select '-- Rename reference table back --'"
$CLICKHOUSE_CLIENT --query "rename table ref_table2 to ref_table"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id"

$CLICKHOUSE_CLIENT --query "select '-- Alter reference table --'"
$CLICKHOUSE_CLIENT --query "alter table ref_table drop column name"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id"

$CLICKHOUSE_CLIENT --query "select '-- Alter alias table --'"
$CLICKHOUSE_CLIENT --query "alter table alias_table add column col Int32 Default 2" 2>&1 | grep -o -F "Code: 1" | uniq
$CLICKHOUSE_CLIENT --query "alter table ref_table add column col Int32 Default 2"
$CLICKHOUSE_CLIENT --query "select * from ref_table order by id"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id"

$CLICKHOUSE_CLIENT --query "select '-- Drops reference table --'"
$CLICKHOUSE_CLIENT --query "drop table ref_table"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id" 2>&1 | grep -o -F "Code: 60" | uniq

$CLICKHOUSE_CLIENT --query "select '-- Re-create reference table with same name --'"
$CLICKHOUSE_CLIENT --query "create table ref_table (id UInt32, b Int32, c UInt32) Engine=MergeTree order by id"
$CLICKHOUSE_CLIENT --query "insert into ref_table values (1, 2, 3)"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id"

$CLICKHOUSE_CLIENT --query "select '-- Truncate alias table --'"
$CLICKHOUSE_CLIENT --query "truncate table alias_table" 2>&1 | grep -o -F "Code: 1" | uniq
$CLICKHOUSE_CLIENT --query "truncate table ref_table"
$CLICKHOUSE_CLIENT --query "select * from ref_table order by id"
$CLICKHOUSE_CLIENT --query "select * from alias_table order by id"

$CLICKHOUSE_CLIENT --query "drop table alias_table"
$CLICKHOUSE_CLIENT --query "drop table ref_table"
