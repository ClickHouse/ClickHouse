#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under `column_and_query_name_matching = 'standard'` a double quote pins a name to exact matching,
# so the quote is part of the identity of a query: it must reach every lookup, and queries that
# differ only in it must not share a query result cache entry.
CLIENT_STANDARD="${CLICKHOUSE_CLIENT} --column_and_query_name_matching=standard"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_quote_identity (x Int32) ENGINE = Memory;
    INSERT INTO t_quote_identity VALUES (1), (2), (3);
    CREATE TABLE t_quote_identity_names (FirstName String) ENGINE = Memory;
    INSERT INTO t_quote_identity_names VALUES ('a');
    CREATE TABLE t_quote_identity_nested (k Int32, data Nested(Name String, name String)) ENGINE = Memory;
    INSERT INTO t_quote_identity_nested VALUES (1, ['N'], ['n']);
    CREATE TABLE t_quote_identity_l (id Int32) ENGINE = Memory;
    INSERT INTO t_quote_identity_l VALUES (1), (2);
    CREATE TABLE t_quote_identity_r (JOINKEY Int32, v String) ENGINE = Memory;
    INSERT INTO t_quote_identity_r VALUES (1, 'a');"

echo '--- quoted lambda arguments are pinned'
${CLIENT_STANDARD} --query 'SELECT arrayMap("X" -> "X" + 1, [1, 2])'
${CLIENT_STANDARD} --query 'SELECT arrayMap(("X", x) -> "X" + x, [1], [2])'
${CLIENT_STANDARD} --query 'SELECT arrayMap("X" -> x + 1, [1, 2])' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- quoted table function arguments stay exact'
${CLIENT_STANDARD} --query "WITH 'CSV' AS FORMAT SELECT * FROM format(format, 'x UInt8', '1')"
${CLIENT_STANDARD} --query "WITH 'CSV' AS FORMAT SELECT * FROM format(\"format\", 'x UInt8', '1')" 2>&1 | grep -oF 'UNKNOWN_FORMAT' | uniq

echo '--- the query result cache distinguishes quoted and unquoted references'
# `X` folds to the column `x` (the double-quoted alias is pinned), `"X"` is the alias.
${CLIENT_STANDARD} --query 'SELECT -x AS "X" FROM t_quote_identity ORDER BY X SETTINGS use_query_cache = 1'
${CLIENT_STANDARD} --query 'SELECT -x AS "X" FROM t_quote_identity ORDER BY "X" SETTINGS use_query_cache = 1'

echo '--- EXCEPT targets folding to one column are ambiguous'
${CLIENT_STANDARD} --query "SELECT * EXCEPT (FirstName, firstname) FROM t_quote_identity_names" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query "SELECT * EXCEPT (firstname, FirstName) FROM t_quote_identity_names" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq

echo '--- an EXCEPT or REPLACE target naming a subcolumn matches the whole name'
${CLIENT_STANDARD} --query 'SELECT * EXCEPT ("data.Name") FROM t_quote_identity_nested FORMAT TSVWithNames'
${CLIENT_STANDARD} --query "SELECT * REPLACE (['R'] AS \"data.Name\") FROM t_quote_identity_nested FORMAT TSVWithNames"
${CLIENT_STANDARD} --query 'SELECT * EXCEPT (`data.name`) FROM t_quote_identity_nested' 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq

echo '--- JOIN USING through a projection alias folds'
# Aliases differing only in character case are rejected already when they are registered.
${CLIENT_STANDARD} --analyzer_compatibility_join_using_top_level_identifier=1 --query "SELECT id AS JoinKey, v FROM t_quote_identity_l JOIN t_quote_identity_r USING (joinkey)"
${CLIENT_STANDARD} --analyzer_compatibility_join_using_top_level_identifier=1 --query "SELECT id AS JoinKey, id + 0 AS joinKEY, v FROM t_quote_identity_l JOIN t_quote_identity_r USING (joinkey)" 2>&1 | grep -oF 'MULTIPLE_EXPRESSIONS_FOR_ALIAS' | uniq
${CLIENT_STANDARD} --analyzer_compatibility_join_using_top_level_identifier=1 --query 'SELECT id AS "JoinKey", v FROM t_quote_identity_l JOIN t_quote_identity_r USING (joinkey)' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- an unqualified name of a temporary table takes precedence over a case sibling in the database'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tmp_quote_identity (v String) ENGINE = Memory; INSERT INTO tmp_quote_identity VALUES ('regular')"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "
    CREATE TEMPORARY TABLE Tmp_Quote_Identity (v String);
    INSERT INTO Tmp_Quote_Identity VALUES ('temporary');
    SELECT v, dummy FROM Tmp_Quote_Identity CROSS JOIN system.one"
