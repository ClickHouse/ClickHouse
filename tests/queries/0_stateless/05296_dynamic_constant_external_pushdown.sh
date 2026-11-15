#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the SQLite integration is not built in the fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Dynamic` constant in a filter pushed down to an external database selects the same rows as the local read.
# A STRICT table, so that the filter on its column is pushed down.

DB_PATH="${CLICKHOUSE_USER_FILES}/05296_dynamic_pushdown_${CLICKHOUSE_DATABASE}.db"
trap 'rm -f "${DB_PATH}"' EXIT
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "CREATE TABLE t (n INTEGER) STRICT; INSERT INTO t VALUES (3), (7), (42);"
chmod ugo+r "${DB_PATH}"

run()
{
    echo "--- $1: external"
    ${CLICKHOUSE_CLIENT} --query "SELECT n FROM sqlite('${DB_PATH}', 't') WHERE $2 ORDER BY n SETTINGS external_table_strict_query = 1"
    echo "--- $1: local"
    ${CLICKHOUSE_CLIENT} --query "SELECT n FROM values('n Int64', (3), (7), (42)) WHERE $2 ORDER BY n"
}

run "IN list" "n IN (CAST(toUInt64(3) AS Dynamic), CAST(toUInt64(7) AS Dynamic))"
