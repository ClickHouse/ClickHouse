#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The pushdown gate cannot rely on the ClickHouse type alone: `ENGINE = SQLite` can point at an arbitrary
# pre-existing table, so a predicate may only be pushed down when the remote column provably compares the
# way ClickHouse does. This test covers the mismatches, all of which would drop rows before the local
# re-filter could recover them:
#   - an explicit `Int64` column over a TEXT-affinity column ('10' < '2' lexicographically);
#   - a `String` column over an INTEGER-affinity column (SQLite compares 10 and 2 numerically, ClickHouse
#     compares the strings '10' and '2' byte-wise, and the two orders disagree);
#   - a `String` column with the NOCASE collation ('a' > 'B' byte-wise, but 'a' < 'b' case-folded);
#   - a `String` column with the RTRIM collation ('a ' > 'a' byte-wise, but equal under RTRIM);
#   - a view, whose expression columns have no affinity at all.
# A well-matched column (Int64 over INTEGER, String over plain TEXT) must stay pushdown-eligible.

DB_PATH="${CLICKHOUSE_TMP}/04637_sqlite_affinity.db"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "
CREATE TABLE t (x TEXT, n INTEGER, s TEXT COLLATE NOCASE, r TEXT COLLATE RTRIM, plain TEXT, num_s INTEGER);
INSERT INTO t VALUES ('10', 10, 'a', 'a ', 'a', '10'), ('2', 2, 'B', 'a', 'B', '2');
CREATE VIEW v AS SELECT x FROM t;
"

${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (x Int64, n Int64, s String, r String, plain String, num_s String) ENGINE = SQLite('${DB_PATH}', 't');

SELECT 'Int64 over TEXT affinity, x > 2 must keep the row storing text 10:';
SELECT x FROM t WHERE x > 2 ORDER BY x;

SELECT 'Int64 over INTEGER affinity stays pushed down and correct:';
SELECT n FROM t WHERE n > 2 ORDER BY n;

SELECT 'String over INTEGER affinity, num_s < ''2'' must keep the row storing 10:';
SELECT num_s FROM t WHERE num_s < '2' ORDER BY num_s;

SELECT 'String with NOCASE collation, s > ''B'' must keep ''a'' (byte-wise order):';
SELECT s FROM t WHERE s > 'B' ORDER BY s;

SELECT 'String with RTRIM collation, r > ''a'' must keep ''a '' (trailing space is significant):';
SELECT r, length(r) FROM t WHERE r > 'a' ORDER BY r;

SELECT 'String over plain TEXT (BINARY collation) stays pushed down and correct:';
SELECT plain FROM t WHERE plain > 'B' ORDER BY plain;

CREATE TABLE v (x Int64) ENGINE = SQLite('${DB_PATH}', 'v');

SELECT 'View columns have no affinity, x > 2 must keep the row storing text 10:';
SELECT x FROM v WHERE x > 2 ORDER BY x;
"

# Prove the well-matched columns are still pushed down (not merely filtered correctly by ClickHouse): the
# query sent to SQLite is logged at trace level and must retain the transformed WHERE for the
# INTEGER-affinity and plain-TEXT columns, while dropping it for the TEXT-affinity Int64 column.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
CREATE TABLE t (x Int64, n Int64, plain String) ENGINE = SQLite('${DB_PATH}', 't');
SELECT n FROM t WHERE n > 2 FORMAT Null;
SELECT plain FROM t WHERE plain > 'B' FORMAT Null;
SELECT x FROM t WHERE x > 2 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT "[^"]*" FROM "t"( WHERE .*)?$'

rm -f "${DB_PATH}"
