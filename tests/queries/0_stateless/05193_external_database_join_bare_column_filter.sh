#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the `sqlite3` binary to build the external database.

# A `WHERE` predicate that is a bare column reference, such as `WHERE b.flag`, used to survive in the
# query built for the *other* side of a join: `removeExpressionsThatDoNotDependOnTableIdentifiers`
# returned early for a top-level expression that is not a `FunctionNode`. For external databases the
# leftover predicate was then written into that side's SQL without its qualifier, so `a`'s scan was
# filtered by `a.flag`, and rows that should have joined disappeared. Spelled as a function,
# `b.flag = 1`, the same condition was dropped correctly, which is why both spellings are compared.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_TMP}/05193.sqlite3"
rm -f "$DB"

sqlite3 "$DB" 'CREATE TABLE t(id INTEGER, id2 INTEGER, flag INTEGER)'
sqlite3 "$DB" 'INSERT INTO t VALUES (1, 10, 0), (50, 1, 1), (2, 20, 1), (60, 2, 1)'

# Both spellings of the filter keep the same two joined rows: `a.id = 1` pairs with the row where
# `id2 = 1` and `a.id = 2` with the row where `id2 = 2`, and both partners have `flag = 1`. The first
# pair has `a.flag = 0`, which is what the leaked predicate used to filter out.
${CLICKHOUSE_LOCAL} --multiquery "
SELECT 'bare column', count() FROM sqlite('${DB}', 't') AS a
JOIN sqlite('${DB}', 't') AS b ON a.id = b.id2 WHERE b.flag;

SELECT 'the same as a function', count() FROM sqlite('${DB}', 't') AS a
JOIN sqlite('${DB}', 't') AS b ON a.id = b.id2 WHERE b.flag = 1;

SELECT 'the rows themselves', a.id, a.flag, b.id2 FROM sqlite('${DB}', 't') AS a
JOIN sqlite('${DB}', 't') AS b ON a.id = b.id2 WHERE b.flag ORDER BY a.id;

-- A filter on the other side of the join, and one on each side.
SELECT 'the other side', count() FROM sqlite('${DB}', 't') AS a
JOIN sqlite('${DB}', 't') AS b ON a.id = b.id2 WHERE a.flag;

SELECT 'both sides', count() FROM sqlite('${DB}', 't') AS a
JOIN sqlite('${DB}', 't') AS b ON a.id = b.id2 WHERE a.flag AND b.flag;

-- The same data locally, as the answer to compare against.
CREATE TABLE t (id Int64, id2 Int64, flag Int64) ENGINE = Memory;
INSERT INTO t VALUES (1, 10, 0), (50, 1, 1), (2, 20, 1), (60, 2, 1);
SELECT 'locally', count() FROM t AS a JOIN t AS b ON a.id = b.id2 WHERE b.flag;
"

rm -f "$DB"
