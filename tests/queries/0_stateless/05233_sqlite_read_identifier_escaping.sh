#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Fast tests don't build external libraries (SQLite)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${USER_FILES_PATH}/05233_sqlite_read_identifier_escaping_${CLICKHOUSE_DATABASE}.db"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_05233_control"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_05233_odd_table_and_column"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_05233_odd_column"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_05233_backslash"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_05233_subquery"
    rm -f "${DB_PATH}"
}
trap cleanup EXIT
cleanup

# SQLite accepts `"` inside a quoted identifier when it is doubled, and treats `\` as a literal byte.
# Create the fixture with SQLite itself so the names are unambiguously legal there.
sqlite3 "${DB_PATH}" 'CREATE TABLE "ta""ble" ("c""1" INTEGER); INSERT INTO "ta""ble" VALUES (42),(43);'
sqlite3 "${DB_PATH}" 'CREATE TABLE plain (v INTEGER); INSERT INTO plain VALUES (7),(8);'
sqlite3 "${DB_PATH}" 'CREATE TABLE oddcol ("c""1" INTEGER); INSERT INTO oddcol VALUES (55);'
sqlite3 "${DB_PATH}" 'CREATE TABLE "a\b" (x INTEGER); INSERT INTO "a\b" VALUES (77);'

chmod ugo+rw "${DB_PATH}"

# The engine reports a mis-quoted identifier as `Code: 591 ... SQL logic error`: `sqlite3_errstr`
# renders only the generic status string, so SQLite's own message never reaches the client, and raw
# stderr carries the database path and the server version. Print the rows on success and exactly one
# stable token on failure.
run()
{
    local out
    if out=$(${CLICKHOUSE_CLIENT} --query="$1" 2>&1); then
        echo "$out"
    else
        local tag
        tag=$(echo "$out" | grep -oF -e 'SQL logic error' -e 'SQLITE_ENGINE_ERROR' | sed -n 1p)
        echo "ERROR: ${tag:-unexpected}"
    fi
}

echo "--- 1 control: plain identifiers, engine table-name form"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_05233_control (v Int32) ENGINE = SQLite('${DB_PATH}', 'plain')"
run "SELECT v FROM test_05233_control ORDER BY v"

echo "--- 2 table and column names contain a double quote, engine table-name form"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_05233_odd_table_and_column (\`c\"1\` Int32) ENGINE = SQLite('${DB_PATH}', 'ta\"ble')"
run "SELECT \`c\"1\` FROM test_05233_odd_table_and_column ORDER BY \`c\"1\`"

echo "--- 3 only the column name contains a double quote"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_05233_odd_column (\`c\"1\` Int32) ENGINE = SQLite('${DB_PATH}', 'oddcol')"
run "SELECT \`c\"1\` FROM test_05233_odd_column"

echo "--- 4 table name contains a backslash"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_05233_backslash (x Int32) ENGINE = SQLite('${DB_PATH}', 'a\\\\b')"
run "SELECT x FROM test_05233_backslash"

echo "--- 5 query(...) form, only the generated wrapper column list is quoted by ClickHouse"
run "SELECT \`c\"1\` FROM sqlite('${DB_PATH}', query('SELECT \"c\"\"1\" FROM \"ta\"\"ble\"')) ORDER BY \`c\"1\`"

echo "--- 6 (SELECT ...) engine argument aliased to a plain name, only the inner text is reserialized"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_05233_subquery (v Int32) ENGINE = SQLite('${DB_PATH}', (SELECT \`c\"1\` AS v FROM \`ta\"ble\`))"
run "SELECT v FROM test_05233_subquery ORDER BY v"

echo "--- 7 sqlite() table function with a (SELECT ...) argument"
run "SELECT \`c\"1\` FROM sqlite('${DB_PATH}', (SELECT \`c\"1\` FROM \`ta\"ble\`)) ORDER BY \`c\"1\`"

echo "--- 8 WHERE predicate pushed down on a double-quoted column"
run "SELECT \`c\"1\` FROM test_05233_odd_table_and_column WHERE \`c\"1\` = 43"

echo "--- 9 negative control: query(...) with a plain alias, ClickHouse quotes nothing odd"
run "SELECT v FROM sqlite('${DB_PATH}', query('SELECT \"c\"\"1\" AS v FROM \"ta\"\"ble\"')) ORDER BY v"
