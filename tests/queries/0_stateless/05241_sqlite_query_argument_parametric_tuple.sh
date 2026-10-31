#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the SQLite library

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `(SELECT ...)` table argument of `sqlite` is re-serialized from the parsed AST and sent to
# SQLite as is. The names of a parametric `tuple('a', 'b')(x, y)` are ClickHouse-only syntax, so
# they must be dropped and the call emitted as a plain row value, as for the pushed-down filters.

DB_PATH="${USER_FILES_PATH}/05241_sqlite_parametric_tuple_${CLICKHOUSE_DATABASE}.db"

cleanup()
{
    rm -f "${DB_PATH}"
}
trap cleanup EXIT
cleanup

python3 - "${DB_PATH}" <<'EOF'
import sys, sqlite3
conn = sqlite3.connect(sys.argv[1])
conn.execute("CREATE TABLE t (id INTEGER, val TEXT)")
conn.execute("INSERT INTO t VALUES (1, 'x')")
conn.execute("INSERT INTO t VALUES (2, 'y')")
conn.commit()
conn.close()
EOF

chmod ugo+r "${DB_PATH}"

echo "--- parametric tuple as a comparison operand in the passed query"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE tuple('a', 'b')(id, val) = (2, 'y'))) ORDER BY id"

echo "--- parametric tuple on the left side of IN in the passed query"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE tuple('a', 'b')(id, val) IN ((1, 'x')))) ORDER BY id"

echo "--- parametric tuple in the pushed-down filter"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', 't') WHERE tuple('a', 'b')(id, val) = (1, 'x') ORDER BY id"
