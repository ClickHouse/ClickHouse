#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The pushdown gate cannot rely on the ClickHouse type alone: `ENGINE = SQLite` can point at an arbitrary
# pre-existing table, so a predicate may only be pushed down when the remote column provably compares the
# way ClickHouse does. Declared affinity is not enough for that - SQLite is dynamically typed per cell, so
# only a STRICT table pins the storage class of every cell to the declared column type. This test covers
# the declared-type and collation mismatches, all of which would drop rows before the local re-filter could
# recover them:
#   - an explicit `Int64` column over a TEXT-declared column ('10' < '2' lexicographically);
#   - a `String` column over an INTEGER-declared column (SQLite compares 10 and 2 numerically, ClickHouse
#     compares the strings '10' and '2' byte-wise, and the two orders disagree);
#   - a `String` column with the NOCASE collation ('a' > 'B' byte-wise, but 'a' < 'b' case-folded);
#   - a `String` column with the RTRIM collation ('a ' > 'a' byte-wise, but equal under RTRIM);
#   - a view, whose expression columns have no affinity at all.
# A well-matched column of a STRICT table (Int64 over INTEGER, String over plain TEXT) must stay
# pushdown-eligible, while the same columns of an ordinary (non-STRICT) table must not. The STRICT columns
# expected to push down are declared NOT NULL: a nullable remote column mapped to a non-Nullable local type
# keeps its predicates local (covered by 04653_sqlite_pushdown_remote_nullability).

DB_PATH="${CLICKHOUSE_TMP}/04637_sqlite_affinity.db"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "
CREATE TABLE t (x TEXT, n INTEGER, s TEXT COLLATE NOCASE, r TEXT COLLATE RTRIM, plain TEXT, num_s INTEGER);
INSERT INTO t VALUES ('10', 10, 'a', 'a ', 'a', '10'), ('2', 2, 'B', 'a', 'B', '2');
CREATE TABLE st (n INTEGER NOT NULL, plain TEXT NOT NULL, s TEXT COLLATE NOCASE) STRICT;
INSERT INTO st VALUES (10, 'a', 'a'), (2, 'B', 'B');
CREATE VIEW v AS SELECT x FROM t;
"

${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (x Int64, n Int64, s String, r String, plain String, num_s String) ENGINE = SQLite('${DB_PATH}', 't');

SELECT 'Int64 over TEXT declared type, x > 2 must keep the row storing text 10:';
SELECT x FROM t WHERE x > 2 ORDER BY x;

SELECT 'Int64 over INTEGER declared type of a non-STRICT table stays correct (filtered locally):';
SELECT n FROM t WHERE n > 2 ORDER BY n;

SELECT 'String over INTEGER declared type, num_s < ''2'' must keep the row storing 10:';
SELECT num_s FROM t WHERE num_s < '2' ORDER BY num_s;

SELECT 'String with NOCASE collation, s > ''B'' must keep ''a'' (byte-wise order):';
SELECT s FROM t WHERE s > 'B' ORDER BY s;

SELECT 'String with RTRIM collation, r > ''a'' must keep ''a '' (trailing space is significant):';
SELECT r, length(r) FROM t WHERE r > 'a' ORDER BY r;

SELECT 'String over plain TEXT of a non-STRICT table stays correct (filtered locally):';
SELECT plain FROM t WHERE plain > 'B' ORDER BY plain;

CREATE TABLE st (n Int64, plain String, s String) ENGINE = SQLite('${DB_PATH}', 'st');

SELECT 'Int64 over INTEGER of a STRICT table stays pushed down and correct:';
SELECT n FROM st WHERE n > 2 ORDER BY n;

SELECT 'String over TEXT (BINARY collation) of a STRICT table stays pushed down and correct:';
SELECT plain FROM st WHERE plain > 'B' ORDER BY plain;

SELECT 'String with NOCASE collation of a STRICT table, s > ''B'' must keep ''a'':';
SELECT s FROM st WHERE s > 'B' ORDER BY s;

CREATE TABLE v (x Int64) ENGINE = SQLite('${DB_PATH}', 'v');

SELECT 'View columns have no affinity, x > 2 must keep the row storing text 10:';
SELECT x FROM v WHERE x > 2 ORDER BY x;
"

# Prove the well-matched STRICT columns are still pushed down (not merely filtered correctly by ClickHouse):
# the query sent to SQLite is logged at trace level and must retain the transformed WHERE for the STRICT
# table's INTEGER and plain-TEXT columns, while dropping it for the same columns of the non-STRICT table,
# for the STRICT NOCASE column and for the TEXT-declared Int64 column.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
CREATE TABLE st (n Int64, plain String, s String) ENGINE = SQLite('${DB_PATH}', 'st');
CREATE TABLE t (x Int64, n Int64, plain String) ENGINE = SQLite('${DB_PATH}', 't');
SELECT n FROM st WHERE n > 2 FORMAT Null;
SELECT plain FROM st WHERE plain > 'B' FORMAT Null;
SELECT s FROM st WHERE s > 'B' FORMAT Null;
SELECT n FROM t WHERE n > 2 FORMAT Null;
SELECT plain FROM t WHERE plain > 'B' FORMAT Null;
SELECT x FROM t WHERE x > 2 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `[^`]*` FROM `(t|st)`( WHERE .*)?$'

rm -f "${DB_PATH}"
