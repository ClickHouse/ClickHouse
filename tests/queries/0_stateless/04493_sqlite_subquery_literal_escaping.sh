#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: SQLite file locking causes spurious failures under concurrent SHOW TABLES

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${USER_FILES_PATH}/04493_sqlite_subquery_escaping_${CLICKHOUSE_DATABASE}.db"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_04493_engine"
    rm -f "${DB_PATH}"
}
trap cleanup EXIT
cleanup

# Create a SQLite database with rows that contain special characters using Python
python3 - "${DB_PATH}" <<'EOF'
import sys, sqlite3
conn = sqlite3.connect(sys.argv[1])
conn.execute("CREATE TABLE t (id INTEGER, val TEXT)")
conn.execute("INSERT INTO t VALUES (1, 'plain')")
conn.execute("INSERT INTO t VALUES (2, ?)", ("it's",))           # single quote
conn.execute("INSERT INTO t VALUES (3, ?)", ("a\tb",))           # tab (0x09)
conn.execute("INSERT INTO t VALUES (4, ?)", ("a\nb",))           # newline (0x0a)
conn.execute("INSERT INTO t VALUES (5, ?)", ("back\\slash",))    # literal backslash
conn.commit()
conn.close()
EOF

chmod ugo+r "${DB_PATH}"

# A `(SELECT ...)` table argument is reserialized by ClickHouse before it is sent to SQLite, so the
# literals inside it must use SQLite escaping (quote doubling), not backslash escaping — the same
# requirement as for pushed-down WHERE predicates.

echo "--- table function: subquery with single-quote literal"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE val = 'it''s')) ORDER BY id"

echo "--- table function: subquery with tab literal"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE val = 'a\tb')) ORDER BY id"

echo "--- table function: subquery with newline literal"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE val = 'a\nb')) ORDER BY id"

echo "--- table function: subquery with backslash literal"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE val = 'back\\\\slash')) ORDER BY id"

echo "--- table function: subquery with IN list of special-character strings"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE val IN ('it''s', 'a\tb', 'back\\\\slash'))) ORDER BY id"

# A single-row multi-column IN set (`(id, val) IN ((2, 'it''s'))`) must keep its outer parentheses when
# reserialized for SQLite. Otherwise it collapses to `(id, val) IN (2, 'it''s')`, which SQLite rejects
# with "IN(...) element has 1 term - expected 2".
echo "--- table function: subquery with multi-column single-row IN"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE (id, val) IN ((2, 'it''s')))) ORDER BY id"

echo "--- table function: subquery with multi-column multi-row IN"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', (SELECT id, val FROM t WHERE (id, val) IN ((1, 'plain'), (2, 'it''s')))) ORDER BY id"

echo "--- engine: subquery with single-quote literal"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_04493_engine (id Int32, val String) ENGINE = SQLite('${DB_PATH}', (SELECT id, val FROM t WHERE val = 'it''s'))"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM test_04493_engine ORDER BY id"
