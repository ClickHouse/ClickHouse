#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Continuation of 04517_column_matching_standard: joins, matchers, views and INTERPOLATE.
CLIENT_STANDARD="${CLICKHOUSE_CLIENT} --column_and_query_name_matching=standard"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_col_match (FirstName String) ENGINE = Memory; INSERT INTO t_col_match VALUES ('a'); CREATE TABLE t_col_siblings (Val Int32, val Int32) ENGINE = Memory; INSERT INTO t_col_siblings VALUES (1, 2); CREATE TABLE t_col_group (Category String, Amount Int32) ENGINE = Memory; INSERT INTO t_col_group VALUES ('x', 1), ('x', 2), ('y', 5); CREATE TABLE t_col_join_l (Id Int32, a Int32) ENGINE = Memory; INSERT INTO t_col_join_l VALUES (1, 10); CREATE TABLE t_col_join_r (ID Int32, b Int32) ENGINE = Memory; INSERT INTO t_col_join_r VALUES (1, 20); CREATE TABLE t_col_join_sib (Id Int32, ID Int32, c Int32) ENGINE = Memory"

echo '--- standard: JOIN USING folds per side, merged key named as written, quoted key pins'
${CLIENT_STANDARD} --query "SELECT * FROM t_col_join_l JOIN t_col_join_r USING (id) FORMAT TSVWithNames; SELECT id, a, b FROM t_col_join_l JOIN t_col_join_r USING (id)"
${CLIENT_STANDARD} --query 'SELECT * FROM t_col_join_l JOIN t_col_join_r USING ("Id")' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query "SELECT * FROM t_col_join_l JOIN t_col_join_sib USING (id)" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_col_join_l JOIN t_col_join_r USING (id)" 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- standard: NATURAL JOIN intersects by folded class, case siblings are ambiguous'
${CLIENT_STANDARD} --query "SELECT * FROM t_col_join_l NATURAL JOIN t_col_join_r FORMAT TSVWithNames"
${CLIENT_STANDARD} --query "SELECT * FROM t_col_join_l NATURAL JOIN t_col_join_sib" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq
# In sensitive mode there are no exact common columns, so NATURAL JOIN degrades to CROSS JOIN
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_col_join_l NATURAL JOIN t_col_join_r FORMAT TSVWithNames"

echo '--- standard: qualified matcher qualifier folds against table aliases and names, quoted qualifier pins'
${CLIENT_STANDARD} --query "SELECT t.* FROM t_col_match AS T FORMAT TSVWithNames"
${CLIENT_STANDARD} --query 'SELECT "t".* FROM t_col_match AS T' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query "SELECT T_COL_MATCH.* FROM t_col_match FORMAT TSVWithNames"
${CLICKHOUSE_CLIENT} --query "SELECT t.* FROM t_col_match AS T" 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- standard: EXCEPT targets fold, quoted targets pin, case siblings are ambiguous'
${CLIENT_STANDARD} --query 'SELECT * EXCEPT (category) FROM t_col_group ORDER BY amount FORMAT TSVWithNames; SELECT * EXCEPT ("category") FROM t_col_group ORDER BY amount LIMIT 1 FORMAT TSVWithNames'
${CLIENT_STANDARD} --query "SELECT * EXCEPT (val) FROM t_col_siblings" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq

echo '--- standard: REPLACE targets fold, case siblings are ambiguous'
${CLIENT_STANDARD} --query "SELECT * REPLACE (a + 5 AS A) FROM t_col_join_l FORMAT TSVWithNames"
${CLIENT_STANDARD} --query "SELECT * REPLACE (1 AS val) FROM t_col_siblings" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq

echo '--- standard: COLUMNS list entries fold, case siblings are ambiguous'
${CLIENT_STANDARD} --query "SELECT COLUMNS(firstname) FROM t_col_match FORMAT TSVWithNames"
${CLIENT_STANDARD} --query "SELECT COLUMNS(val) FROM t_col_siblings" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq

echo '--- standard: quoted subquery projection definitions pin, quoted references still match'
${CLIENT_STANDARD} --query 'SELECT myalias FROM (SELECT 1 AS "MyAlias")' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'SELECT "MyAlias" FROM (SELECT 1 AS "MyAlias"); SELECT myalias FROM (SELECT 1 AS MyAlias) FORMAT TSVWithNames'
${CLIENT_STANDARD} --query 'SELECT x, y FROM (SELECT 1, 2) AS t(X, "Y")' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'SELECT x, "Y" FROM (SELECT 1, 2) AS t(X, "Y") FORMAT TSVWithNames'

echo '--- standard: inlined views carry pins, quotes are not persisted across metadata reload'
${CLICKHOUSE_CLIENT} --query 'CREATE VIEW v_col_pin AS SELECT 1 AS "MyAlias"'
${CLIENT_STANDARD} --query 'SELECT myalias FROM v_col_pin'
${CLIENT_STANDARD} --analyzer_inline_views=1 --query 'SELECT myalias FROM v_col_pin' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --analyzer_inline_views=1 --query 'SELECT "MyAlias" FROM v_col_pin'
${CLICKHOUSE_CLIENT} --query 'DETACH TABLE v_col_pin; ATTACH TABLE v_col_pin'
${CLIENT_STANDARD} --analyzer_inline_views=1 --query 'SELECT myalias FROM v_col_pin'

echo '--- standard: recursive CTE quoted column definitions pin inside recursive members'
${CLIENT_STANDARD} --query 'WITH RECURSIVE cte("MyCol") AS (SELECT 1 UNION ALL SELECT mycol + 1 FROM cte WHERE mycol < 3) SELECT * FROM cte' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'WITH RECURSIVE cte("MyCol") AS (SELECT 1 UNION ALL SELECT "MyCol" + 1 FROM cte WHERE "MyCol" < 3) SELECT * FROM cte'
${CLIENT_STANDARD} --query 'WITH RECURSIVE cte AS (SELECT 1 AS "MyCol" UNION ALL SELECT mycol + 1 FROM cte WHERE mycol < 3) SELECT * FROM cte' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'WITH RECURSIVE cte AS (SELECT 1 AS "MyCol" UNION ALL SELECT "MyCol" + 1 FROM cte WHERE "MyCol" < 3) SELECT * FROM cte; WITH RECURSIVE cte AS (SELECT 1 AS MyCol UNION ALL SELECT mycol + 1 FROM cte WHERE mycol < 3) SELECT * FROM cte'

echo '--- standard: quoted CTE names pin qualifier and table-expression lookups'
${CLIENT_STANDARD} --query 'WITH "MyCte" AS (SELECT 1 AS x) SELECT mycte.* FROM "MyCte"' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'WITH "MyCte" AS (SELECT 1 AS x) SELECT mycte.x FROM "MyCte"' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'WITH "MyCte" AS (SELECT 1 AS x) SELECT "MyCte".x, "MyCte".* FROM "MyCte"'
${CLIENT_STANDARD} --query 'WITH MyCte AS (SELECT 1 AS x) SELECT mycte.x, MYCTE.* FROM myCTE'

echo '--- standard: quoted recursive CTE name pins the self-reference qualifier'
${CLIENT_STANDARD} --query 'WITH RECURSIVE "MyCte" AS (SELECT 1 AS n UNION ALL SELECT mycte.n + 1 FROM "MyCte" WHERE mycte.n < 3) SELECT max(n) FROM "MyCte"' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'WITH RECURSIVE "MyCte" AS (SELECT 1 AS n UNION ALL SELECT "MyCte".n + 1 FROM "MyCte" WHERE "MyCte".n < 3) SELECT max(n) FROM "MyCte"'

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_col_interp (x UInt64, Val UInt64) ENGINE = Memory; INSERT INTO t_col_interp VALUES (1, 10), (3, 10)"

echo '--- standard: INTERPOLATE targets fold to the canonical column, quoted targets pin'
${CLIENT_STANDARD} --query "SELECT x, Val FROM t_col_interp ORDER BY x ASC WITH FILL FROM 1 TO 4 INTERPOLATE (val AS Val + 1); SELECT Val FROM (SELECT x, Val FROM t_col_interp ORDER BY x ASC WITH FILL FROM 1 TO 4 INTERPOLATE (val AS Val + 1))"
${CLIENT_STANDARD} --query 'SELECT x, Val FROM t_col_interp ORDER BY x ASC WITH FILL FROM 1 TO 4 INTERPOLATE ("val" AS Val + 1)' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLICKHOUSE_CLIENT} --query "SELECT x, Val FROM t_col_interp ORDER BY x ASC WITH FILL FROM 1 TO 4 INTERPOLATE (Val AS Val + 1)"
${CLICKHOUSE_CLIENT} --query "SELECT x, Val FROM t_col_interp ORDER BY x ASC WITH FILL FROM 1 TO 4 INTERPOLATE (val AS Val + 1)" 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

${CLICKHOUSE_CLIENT} --query "DROP VIEW v_col_pin; DROP TABLE t_col_interp; DROP TABLE t_col_match; DROP TABLE t_col_siblings; DROP TABLE t_col_group; DROP TABLE t_col_join_l; DROP TABLE t_col_join_r; DROP TABLE t_col_join_sib"
