#!/usr/bin/env bash
# An alias replaces the table name of the table expression it is given to, so a qualifier that is the
# alias of one side of a join and merely the name of a table on the other side refers to the alias.
# Without that precedence the two readings were equally good: a three-way join whose first table is
# aliased with the name of another table of the query threw `AMBIGUOUS_IDENTIFIER`, and a two-way join
# resolved the qualifier to the table name of the left side instead of the right side's alias.
# The tables are named with the database, because an unqualified name in `FROM` that matches an alias of
# the same query resolves to that table expression instead of the table.

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
"

echo 'and the alias of the second table wins over the name of the first one'
${CLICKHOUSE_CLIENT} -q "SELECT t1.rev FROM ${db}.t1 AS a INNER JOIN ${db}.t0 AS t1 ON a.id = t1.id"

echo 'the same for a comma join'
${CLICKHOUSE_CLIENT} -q "SELECT t1.rev FROM ${db}.t1 AS a, ${db}.t0 AS t1 WHERE a.id = t1.id"

echo 'a qualifier that is only a table name still resolves'
${CLICKHOUSE_CLIENT} -q "SELECT t1.rev FROM ${db}.t0 AS left_0 INNER JOIN ${db}.t1 ON left_0.id = t1.id"

echo 'an unqualified column of both sides is still ambiguous'
${CLICKHOUSE_CLIENT} -q "
SELECT rev FROM ${db}.t0 AS a
INNER JOIN ${db}.t1 AS b ON a.id = b.id
INNER JOIN ${db}.t1 AS c ON a.id = c.id
" 2>&1 | grep -c -m1 AMBIGUOUS_IDENTIFIER
