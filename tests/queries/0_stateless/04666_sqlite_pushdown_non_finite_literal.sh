#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_pushdown_non_finite.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

# SQLite accepts infinity in a `STRICT REAL` column (the overflowing literal `1e999` evaluates to `+inf`), but
# parses bare `inf` and `nan` in SQL text as identifiers. ClickHouse formats its corresponding Float64 literals
# with those bare tokens, so predicates containing them must stay local instead of making SQLite reject an
# otherwise valid query. The `IN` case verifies that the compatibility check recurses through tuple fields.
sqlite3 "$DB_PATH" "
    CREATE TABLE t(x REAL NOT NULL) STRICT;
    INSERT INTO t VALUES (1e999), (1.5);
"

${CLICKHOUSE_LOCAL} --multiquery --query="
    CREATE TABLE t (x Float64) ENGINE = SQLite('$DB_PATH', 't');

    SELECT 'Infinity through the SQLite storage engine:';
    SELECT count() FROM t WHERE x = inf;

    SELECT 'Infinity through the sqlite table function:';
    SELECT count() FROM sqlite('$DB_PATH', 't') WHERE x = inf;

    SELECT 'NaN is also kept local instead of producing invalid SQLite SQL:';
    SELECT count() FROM t WHERE x = nan;

    SELECT 'A tuple IN set containing infinity is kept local:';
    SELECT count() FROM t WHERE x IN (inf, 1.5);
"

# Non-finite scalar and tuple literals are omitted from the remote WHERE, while a finite predicate remains
# pushdown-eligible. This checks the generated SQLite SQL rather than only relying on the final query result.
${CLICKHOUSE_LOCAL} --send_logs_level=trace --multiquery --query="
    CREATE TABLE t (x Float64) ENGINE = SQLite('$DB_PATH', 't');
    SELECT x FROM t WHERE x = inf FORMAT Null;
    SELECT x FROM t WHERE x IN (inf, 1.5) FORMAT Null;
    SELECT x FROM t WHERE x = 1.5 FORMAT Null;
" 2>&1 | grep -oE 'Query: SELECT `x` FROM `t`( WHERE .*)?$'
