#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: SQLite file locking causes spurious failures under concurrent SHOW TABLES

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${USER_FILES_PATH}/04141_sqlite_escaping_${CLICKHOUSE_DATABASE}.db"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_04141_engine"
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

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_04141_engine (id Int32, val String) ENGINE = SQLite('${DB_PATH}', 't')"

echo "--- engine: exact match on single-quote string"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM test_04141_engine WHERE val = 'it''s' ORDER BY id"

echo "--- engine: exact match on tab"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM test_04141_engine WHERE val = 'a\tb' ORDER BY id"

echo "--- engine: exact match on newline"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM test_04141_engine WHERE val = 'a\nb' ORDER BY id"

echo "--- engine: exact match on backslash"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM test_04141_engine WHERE val = 'back\\\\slash' ORDER BY id"

echo "--- table function: exact match on single-quote string"
${CLICKHOUSE_CLIENT} --query="SELECT id, val FROM sqlite('${DB_PATH}', 't') WHERE val = 'it''s' ORDER BY id"

echo "--- table function: exact match on tab"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE val = 'a\tb' ORDER BY id"

echo "--- table function: exact match on newline"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE val = 'a\nb' ORDER BY id"

echo "--- table function: exact match on backslash"
${CLICKHOUSE_CLIENT} --query="SELECT id FROM sqlite('${DB_PATH}', 't') WHERE val = 'back\\\\slash' ORDER BY id"
