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

echo '--- standard: expression aliases fold, quoted alias definitions pin, case-sibling definitions rejected'
${CLIENT_STANDARD} --query "SELECT 1 AS Foo, foo"
${CLIENT_STANDARD} --query 'SELECT 1 AS Bar, "Bar"'
${CLIENT_STANDARD} --query "SELECT 1 AS Foo, 2 AS foo" 2>&1 | grep -oF 'MULTIPLE_EXPRESSIONS_FOR_ALIAS' | uniq
${CLIENT_STANDARD} --query 'SELECT 1 AS "Foo", foo' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLICKHOUSE_CLIENT} --query "SELECT 1 AS Foo, 2 AS foo"

echo '--- standard: CTE names fold, quoted CTE definitions pin, case-sibling definitions rejected'
${CLIENT_STANDARD} --query "WITH q AS (SELECT 1 AS x) SELECT * FROM Q"
${CLIENT_STANDARD} --query 'WITH "Q" AS (SELECT 1) SELECT * FROM q' 2>&1 | grep -oF 'UNKNOWN_TABLE' | uniq
${CLIENT_STANDARD} --query 'WITH "Q" AS (SELECT 1 AS x) SELECT * FROM "Q"'
${CLIENT_STANDARD} --query "WITH a AS (SELECT 1), A AS (SELECT 2) SELECT * FROM a" 2>&1 | grep -oF 'MULTIPLE_EXPRESSIONS_FOR_ALIAS' | uniq
${CLICKHOUSE_CLIENT} --query "WITH q AS (SELECT 1) SELECT * FROM Q" 2>&1 | grep -oF 'UNKNOWN_TABLE' | uniq

echo '--- standard: lambda arguments fold, case-sibling arguments rejected'
${CLIENT_STANDARD} --query "SELECT arrayMap(X -> x + 1, [1, 2])"
${CLIENT_STANDARD} --query "SELECT arrayMap((X, x) -> X, [1], [2])" 2>&1 | grep -oF 'BAD_ARGUMENTS' | uniq
${CLICKHOUSE_CLIENT} --query "SELECT arrayMap(X -> x + 1, [1, 2])" 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- standard: alias vs column precedence follows the exact-mode order'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_col_prec (a Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_col_prec VALUES (10)"
${CLIENT_STANDARD} --query "SELECT 5 AS A, a FROM t_col_prec"
${CLICKHOUSE_CLIENT} --query "SELECT 5 AS a, a FROM t_col_prec"
${CLIENT_STANDARD} --prefer_column_name_to_alias=1 --query "SELECT 5 AS A, a FROM t_col_prec"
# The exact-spelling equivalent resolves to the column the same way, then fails on duplicate
# output column names with different types; that projection-level error exists on master too.
${CLICKHOUSE_CLIENT} --prefer_column_name_to_alias=1 --query "SELECT 5 AS a, a FROM t_col_prec" 2>&1 | grep -oF 'AMBIGUOUS_COLUMN_NAME' | uniq

echo '--- standard: window names fold, quoted definitions pin, case-sibling definitions rejected'
${CLIENT_STANDARD} --query "SELECT count() OVER W FROM t_col_group WINDOW w AS (PARTITION BY Category) ORDER BY ALL"
${CLIENT_STANDARD} --query 'SELECT count() OVER w FROM t_col_group WINDOW "W" AS (PARTITION BY Category)' 2>&1 | grep -oF 'BAD_ARGUMENTS' | uniq
${CLIENT_STANDARD} --query "SELECT 1 FROM t_col_group WINDOW w AS (), W AS ()" 2>&1 | grep -oF 'BAD_ARGUMENTS' | uniq
${CLICKHOUSE_CLIENT} --query "SELECT count() OVER W FROM t_col_group WINDOW w AS (PARTITION BY Category)" 2>&1 | grep -oF 'BAD_ARGUMENTS' | uniq

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_match"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_siblings"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_tuple"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_group"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_col_prec"
