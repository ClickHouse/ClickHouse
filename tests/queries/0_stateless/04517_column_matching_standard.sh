#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT_STANDARD="${CLICKHOUSE_CLIENT} --column_and_query_name_matching=standard"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_col_match (FirstName String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_col_match VALUES ('a')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_col_siblings (Val Int32, val Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_col_siblings VALUES (1, 2)"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_col_tuple (Data Tuple(Name String)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_col_tuple VALUES (('n'))"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_col_group (Category String, Amount Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_col_group VALUES ('x', 1), ('x', 2), ('y', 5)"

echo '--- standard: unquoted spellings fold, header shows the canonical column'
${CLIENT_STANDARD} --query "SELECT firstname FROM t_col_match FORMAT TSVWithNames"
${CLIENT_STANDARD} --query "SELECT FIRSTNAME FROM t_col_match FORMAT TSVWithNames"
${CLIENT_STANDARD} --query "SELECT FirstName FROM t_col_match FORMAT TSVWithNames"

echo '--- standard: case siblings are ambiguous for any unquoted spelling, including the exact one'
${CLIENT_STANDARD} --query "SELECT Val FROM t_col_siblings" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query "SELECT val FROM t_col_siblings" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query "SELECT VAL FROM t_col_siblings" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq

echo '--- standard: double quotes pin the exact spelling'
${CLIENT_STANDARD} --query 'SELECT "Val" FROM t_col_siblings FORMAT TSVWithNames'
${CLIENT_STANDARD} --query 'SELECT "val" FROM t_col_siblings FORMAT TSVWithNames'
${CLIENT_STANDARD} --query 'SELECT "firstname" FROM t_col_match' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- standard: tuple subcolumn suffixes fold, quoted suffix pins'
${CLIENT_STANDARD} --query "SELECT Data.name FROM t_col_tuple FORMAT TSVWithNames"
${CLIENT_STANDARD} --query "SELECT data.Name FROM t_col_tuple FORMAT TSVWithNames"
${CLIENT_STANDARD} --query 'SELECT Data."name" FROM t_col_tuple' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- sensitive default: exact spellings work, folded spellings fail'
${CLICKHOUSE_CLIENT} --query "SELECT FirstName FROM t_col_match FORMAT TSVWithNames"
${CLICKHOUSE_CLIENT} --query "SELECT firstname FROM t_col_match" 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLICKHOUSE_CLIENT} --query "SELECT Val, val FROM t_col_siblings"
${CLICKHOUSE_CLIENT} --query "SELECT Data.Name FROM t_col_tuple"
${CLICKHOUSE_CLIENT} --query "SELECT Data.name FROM t_col_tuple" 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- standard: WHERE / GROUP BY / ORDER BY fold like SELECT'
${CLIENT_STANDARD} --query "SELECT firstname FROM t_col_match WHERE FIRSTNAME = 'a'"
${CLIENT_STANDARD} --query "SELECT category, sum(amount) FROM t_col_group GROUP BY CATEGORY ORDER BY category"
${CLIENT_STANDARD} --query "SELECT Category FROM t_col_group WHERE AMOUNT > 1 ORDER BY amount DESC"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_match"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_siblings"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_tuple"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_group"
