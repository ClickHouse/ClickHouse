#!/usr/bin/env bash

# A referenced WITH subquery is evaluated even when the analyzer does not
# substitute it before EXPLAIN's nested-view access check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

reader="reader_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${reader};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (a UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW ${CLICKHOUSE_DATABASE}.v AS SELECT a FROM ${CLICKHOUSE_DATABASE}.base;
CREATE USER ${reader};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v TO ${reader};
"

run() {
    local label="$1"
    local query="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${reader}" --query "SET enable_analyzer = 1, enable_global_with_statement = 0; ${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

query="WITH c AS (SELECT a FROM v) SELECT a FROM c"
run "SELECT" "${query}"
run "EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${query}"
run "EXPLAIN AST optimize=1" "EXPLAIN AST optimize = 1 ${query}"

# Each CTE is declared before its use, and the access walker must keep
# scanning newly discovered dependencies until it reaches the view.
chained_query="WITH b AS (SELECT a FROM v), a AS (SELECT a FROM b), z AS (SELECT a FROM a) SELECT a FROM z"
run "Chained SELECT" "${chained_query}"
run "Chained EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${chained_query}"
run "Chained EXPLAIN AST optimize=1" "EXPLAIN AST optimize = 1 ${chained_query}"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${reader};
"
