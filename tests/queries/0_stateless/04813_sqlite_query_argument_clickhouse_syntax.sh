#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the SQLite library

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `(SELECT ...)` table argument of `sqlite` is re-serialized from the parsed AST and sent to
# SQLite as is. Expressions that only have a ClickHouse-specific text form must not leak into that
# SQL: an explicit `tuple` call with two or more arguments has to be emitted as a parenthesized row
# value, while `tuple` with fewer than two arguments, `array` / `map` calls and `Array` / `Map`
# literals have no SQLite form at all and must be rejected by ClickHouse instead of failing with
# an obscure error on the SQLite side.

DB_PATH="${USER_FILES_PATH}/04813_sqlite_query_argument_${CLICKHOUSE_DATABASE}.db"

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

echo "--- explicit tuple call is sent as a row value"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE tuple(id, val) IN ((1, 'x')))) ORDER BY id"

echo "--- explicit tuple call as a single-row IN set keeps its outer parentheses"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE tuple(id, val) IN (tuple(1, 'x')))) ORDER BY id"

echo "--- row value is valid as a comparison operand"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE (id, val) = (2, 'y'))) ORDER BY id"

echo "--- tuple of predicates in WHERE is lowered to a conjunction"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE (id > 1, val = 'y'))) ORDER BY id"

echo "--- tuple in the SELECT list is rejected by ClickHouse (SQLite: row value misused)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', (SELECT tuple(id, val) FROM t))" 2>&1 | grep -q "BAD_ARGUMENTS" && echo "rejected"

echo "--- tuple literal in the SELECT list is rejected by ClickHouse"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM sqlite('${DB_PATH}', (SELECT (1, 'x') FROM t))" 2>&1 | grep -q "BAD_ARGUMENTS" && echo "rejected"

echo "--- single-element tuple call is rejected by ClickHouse"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', (SELECT id FROM t WHERE tuple(val) = tuple('x')))" 2>&1 | grep -q "BAD_ARGUMENTS" && echo "rejected"

echo "--- Array literal is rejected by ClickHouse"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', (SELECT id FROM t WHERE val IN ['x', 'y']))" 2>&1 | grep -q "BAD_ARGUMENTS" && echo "rejected"

echo "--- array function call is rejected by ClickHouse"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', (SELECT id FROM t WHERE has(array(val), 'x')))" 2>&1 | grep -q "BAD_ARGUMENTS" && echo "rejected"

echo "--- map function call is rejected by ClickHouse"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', (SELECT id FROM t WHERE map(val, 1)['x'] = 1))" 2>&1 | grep -q "BAD_ARGUMENTS" && echo "rejected"
