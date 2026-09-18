#!/usr/bin/env bash
# An alias replaces the table name of the table expression it is given to, so a qualifier that is the
# alias of one side of a join and merely the name of a table on the other side refers to the alias.
# Without that precedence the two readings were equally good: a three-way join whose first table is
# aliased with the name of another table of the query threw `AMBIGUOUS_IDENTIFIER`, and a two-way join
# resolved the qualifier to the table name of the left side instead of the right side's alias.
# The tables are named with the database, because an unqualified name in `FROM` that matches an alias of
# the same query resolves to that table expression instead of the table. The queries pin the analyzer,
# whose resolution this is about; the old analyzer reads such a qualifier as the alias already.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t0 (id UInt32, rev UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t1 (id UInt32, rev UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t0 VALUES (1, 10);
INSERT INTO t1 VALUES (1, 20);
"

echo 'the alias of the first table wins over the name of the third one'
${CLICKHOUSE_CLIENT} -q "
SELECT t1.rev FROM ${db}.t0 AS t1
INNER JOIN ${db}.t0 AS right_0 ON t1.id = right_0.id
INNER JOIN ${db}.t1 AS right_1 ON t1.id = right_1.id
SETTINGS enable_analyzer = 1
"

echo 'and the alias of the second table wins over the name of the first one'
${CLICKHOUSE_CLIENT} -q "SELECT t1.rev FROM ${db}.t1 AS a INNER JOIN ${db}.t0 AS t1 ON a.id = t1.id SETTINGS enable_analyzer = 1"

echo 'the same for a comma join'
${CLICKHOUSE_CLIENT} -q "SELECT t1.rev FROM ${db}.t1 AS a, ${db}.t0 AS t1 WHERE a.id = t1.id SETTINGS enable_analyzer = 1"

echo 'a qualifier that is only a table name still resolves'
${CLICKHOUSE_CLIENT} -q "SELECT t1.rev FROM ${db}.t0 AS left_0 INNER JOIN ${db}.t1 ON left_0.id = t1.id SETTINGS enable_analyzer = 1"

echo 'an unqualified column of both sides is still ambiguous'
${CLICKHOUSE_CLIENT} -q "
SELECT rev FROM ${db}.t0 AS a
INNER JOIN ${db}.t1 AS b ON a.id = b.id
INNER JOIN ${db}.t1 AS c ON a.id = c.id
SETTINGS enable_analyzer = 1
" 2>&1 | grep -c -m1 AMBIGUOUS_IDENTIFIER

# A subcolumn of a subquery projection or of an `ALIAS` column is resolved through a `getSubcolumn`
# wrapper around the column rather than as a column itself; the precedence has to look through it.

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE s0 (id UInt32, x Tuple(y UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE s1 (id UInt32, x Tuple(y UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO s0 VALUES (1, (10));
INSERT INTO s1 VALUES (1, (20));
CREATE TABLE a0 (id UInt32, v UInt32, x Tuple(y UInt32) ALIAS tuple(v)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE a1 (id UInt32, v UInt32, x Tuple(y UInt32) ALIAS tuple(v)) ENGINE = MergeTree ORDER BY id;
INSERT INTO a0 (id, v) VALUES (1, 10);
INSERT INTO a1 (id, v) VALUES (1, 20);
"

echo 'a tuple subcolumn of a table'
${CLICKHOUSE_CLIENT} -q "
SELECT s1.x.y FROM ${db}.s0 AS s1
INNER JOIN ${db}.s0 AS right_0 ON s1.id = right_0.id
INNER JOIN ${db}.s1 AS right_1 ON s1.id = right_1.id
SETTINGS enable_analyzer = 1
"
${CLICKHOUSE_CLIENT} -q "SELECT s1.x.y FROM ${db}.s1 AS a INNER JOIN ${db}.s0 AS s1 ON a.id = s1.id SETTINGS enable_analyzer = 1"

echo 'a tuple subcolumn of a subquery projection'
${CLICKHOUSE_CLIENT} -q "
SELECT s1.x.y FROM (SELECT id, x FROM ${db}.s0) AS s1
INNER JOIN ${db}.s0 AS right_0 ON s1.id = right_0.id
INNER JOIN ${db}.s1 AS right_1 ON s1.id = right_1.id
SETTINGS enable_analyzer = 1
"
${CLICKHOUSE_CLIENT} -q "SELECT s1.x.y FROM ${db}.s1 AS a INNER JOIN (SELECT id, x FROM ${db}.s0) AS s1 ON a.id = s1.id SETTINGS enable_analyzer = 1"

echo 'a tuple subcolumn of an ALIAS column'
${CLICKHOUSE_CLIENT} -q "
SELECT a1.x.y FROM ${db}.a0 AS a1
INNER JOIN ${db}.a0 AS right_0 ON a1.id = right_0.id
INNER JOIN ${db}.a1 AS right_1 ON a1.id = right_1.id
SETTINGS enable_analyzer = 1
"
${CLICKHOUSE_CLIENT} -q "SELECT a1.x.y FROM ${db}.a1 AS a INNER JOIN ${db}.a0 AS a1 ON a.id = a1.id SETTINGS enable_analyzer = 1"

# A CTE or a temporary table cannot be named with a database, so a reference to one that is spelled like
# an alias of the same query is the aliased table expression itself and never competes with the alias.
# The qualifier binds to the alias on both sides then, and the query is a self-join of the aliased table.

echo 'a CTE named like the alias is shadowed by it in FROM'
${CLICKHOUSE_CLIENT} -q "
WITH cte AS (SELECT * FROM ${db}.t1)
SELECT cte.rev, right_1.rev FROM ${db}.t0 AS cte
INNER JOIN ${db}.t0 AS right_0 ON cte.id = right_0.id
INNER JOIN cte AS right_1 ON cte.id = right_1.id
SETTINGS enable_analyzer = 1
"
${CLICKHOUSE_CLIENT} -q "
WITH cte AS MATERIALIZED (SELECT * FROM ${db}.t1)
SELECT cte.rev, a.rev FROM cte AS a INNER JOIN ${db}.t0 AS cte ON a.id = cte.id
SETTINGS enable_analyzer = 1, enable_materialized_cte = 1
"
