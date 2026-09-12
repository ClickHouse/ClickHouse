#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the SQLite integration is not built in the fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Keyed on the test database so parallel runs never share a SQLite file.
DB_PATH="${CLICKHOUSE_USER_FILES}/05138_dynamic_pushdown_${CLICKHOUSE_DATABASE}.db"
trap 'rm -f "${DB_PATH}"' EXIT
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "CREATE TABLE t (field TEXT); INSERT INTO t VALUES ('7'), ('3'), ('x');"
chmod ugo+r "${DB_PATH}"

# `Enum8('7' = 3)` writes its value as the number 3 but compares against a `String` as the name '7',
# so the two select different rows of this table. An external database has no `Dynamic`: it reads
# whatever literal is pushed as text, so pushing the named member (which folds to '7') makes the
# pushed predicate select rows the re-applied ClickHouse predicate then rejects, and the answer is
# a lost row rather than a slower query. Each shape is read twice: through the external table (where
# the predicate is pushed down) and over the same three rows locally (where it is not).
ENUM="CAST(CAST('7', 'Enum8(\\'7\\' = 3)') AS Dynamic)"
ENUM2="CAST(CAST('3', 'Enum8(\\'3\\' = 4)') AS Dynamic)"
LOCAL="values('field String', ('7'), ('3'), ('x'))"

run()
{
    echo "--- $1: external"
    ${CLICKHOUSE_CLIENT} --query "SELECT field FROM sqlite('${DB_PATH}', 't') WHERE $2 ORDER BY field SETTINGS enable_analyzer = 1"
    echo "--- $1: local"
    ${CLICKHOUSE_CLIENT} --query "SELECT field FROM ${LOCAL} WHERE $2 ORDER BY field SETTINGS enable_analyzer = 1"
}

# `=` is the one shape where the two reads differ, and they differ the same way on master: the pushed
# `"field" = 3` is not a superset of `field = '7'`, so the row is dropped before ClickHouse sees it.
# That is pre-existing and out of scope here; the cell is kept so a change to it cannot pass silently.
run "="       "field = ${ENUM}"
run "IN"      "field IN (${ENUM})"
run "IN list" "field IN (${ENUM}, ${ENUM2})"
run "NOT IN"  "field NOT IN (${ENUM})"
