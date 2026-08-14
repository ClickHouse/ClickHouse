#!/usr/bin/env bash

# A nested SELECT is checked independently to ensure a regular view's base-table
# permissions match real execution. It must nevertheless retain the enclosing
# WITH binding: otherwise a CTE named `v` is mistaken for the catalog view `v`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

reader="reader_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${reader};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (a UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (2);
CREATE VIEW ${CLICKHOUSE_DATABASE}.v AS SELECT a FROM ${CLICKHOUSE_DATABASE}.base;
CREATE USER ${reader};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v TO ${reader};
"

run() {
    local settings="$1"
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${reader}" --query "${settings} ${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

query="WITH v AS (SELECT 1 AS a) SELECT a FROM (SELECT a FROM v)"

echo "-- The legacy interpreter applies enclosing WITH subqueries"
run "SET enable_analyzer = 0;" "SELECT" "${query}"
run "SET enable_analyzer = 0;" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${query}"
run "SET enable_analyzer = 0;" "EXPLAIN AST optimize=1" "EXPLAIN AST optimize = 1 ${query}"

echo "-- The analyzer applies them when enable_global_with_statement is enabled"
run "SET enable_analyzer = 1, enable_global_with_statement = 1;" "SELECT" "${query}"
run "SET enable_analyzer = 1, enable_global_with_statement = 1;" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${query}"
run "SET enable_analyzer = 1, enable_global_with_statement = 1;" "EXPLAIN AST optimize=1" "EXPLAIN AST optimize = 1 ${query}"

echo "-- Without that setting the analyzer resolves the catalog view and denies it"
run "SET enable_analyzer = 1, enable_global_with_statement = 0;" "SELECT" "${query}"
run "SET enable_analyzer = 1, enable_global_with_statement = 0;" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${query}"
run "SET enable_analyzer = 1, enable_global_with_statement = 0;" "EXPLAIN AST optimize=1" "EXPLAIN AST optimize = 1 ${query}"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${reader};
"
