#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQLite is dynamically typed per cell: in an ordinary (non-STRICT) table the declared column type only
# sets an affinity applied to new inserts, and any cell can carry any runtime storage class. SQLite
# evaluates a pushed-down predicate against that storage class, while the ClickHouse read path converts the
# cell to the ClickHouse column type, so the two sides can disagree even when the declared type matches:
#   - an INTEGER-declared column holding the TEXT cell 'abc' fails the read of an Int64 column, but on the
#     SQLite side x = 0 is false for a TEXT cell, so a pushed-down x = 0 would silently drop the row instead;
#   - a BLOB-declared column holding x'616263' is read by the String path as 'abc', but on the SQLite side
#     s = 'abc' is false because a BLOB cell never equates with a TEXT literal - for the same reason a
#     BLOB column of a STRICT table is not pushdown-eligible either;
#   - an ANY column of a STRICT table places no constraint on its cells at all.
# Such predicates must stay local. The trace log of the queries sent to SQLite proves no WHERE was pushed.

DB_PATH="${CLICKHOUSE_TMP}/04638_sqlite_storage_class.db"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "
CREATE TABLE t (x INTEGER, s BLOB);
INSERT INTO t VALUES ('abc', x'616263'), (1, x'7A');
CREATE TABLE st (b BLOB, a ANY) STRICT;
INSERT INTO st VALUES (x'616263', 'abc'), (x'7A', 1);
"

${CLICKHOUSE_LOCAL} --query="
CREATE TABLE t (x Int64, s String) ENGINE = SQLite('${DB_PATH}', 't');
CREATE TABLE st (b String, a Int64) ENGINE = SQLite('${DB_PATH}', 'st');

SELECT 'Int64 over INTEGER declared type holding the TEXT cell abc, x = 0 must stay local and fail the read:';
SELECT x, s FROM t WHERE x = 0; -- { serverError INCORRECT_DATA }

SELECT 'String over BLOB declared type, s = abc must keep the row:';
SELECT s FROM t WHERE s = 'abc';

SELECT 'String over the BLOB column of a STRICT table, b = abc must keep the row:';
SELECT b FROM st WHERE b = 'abc';

SELECT 'Int64 over the ANY column of a STRICT table holding the TEXT cell abc, a = 0 must stay local and fail the read:';
SELECT a FROM st WHERE a = 0; -- { serverError INCORRECT_DATA }
"

# None of these predicates may reach SQLite: every query sent to it must have no WHERE clause.
# An error hint makes the client set send_logs_level = 'fatal' for the whole script, so the trace level is set in it.
${CLICKHOUSE_LOCAL} --query="
SET send_logs_level = 'trace';
CREATE TABLE t (x Int64, s String) ENGINE = SQLite('${DB_PATH}', 't');
CREATE TABLE st (b String, a Int64) ENGINE = SQLite('${DB_PATH}', 'st');
SELECT x FROM t WHERE x = 0 FORMAT Null; -- { serverError INCORRECT_DATA }
SELECT s FROM t WHERE s = 'abc' FORMAT Null;
SELECT b FROM st WHERE b = 'abc' FORMAT Null;
SELECT a FROM st WHERE a = 0 FORMAT Null; -- { serverError INCORRECT_DATA }
" 2>&1 | grep -oE 'Query: SELECT `[^`]*` FROM `(t|st)`( WHERE .*)?$'

rm -f "${DB_PATH}"
