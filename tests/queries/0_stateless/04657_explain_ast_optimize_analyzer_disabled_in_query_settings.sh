#!/usr/bin/env bash

# The `SELECT` grant on a parameterized view object is required only when the explained query would
# really run with the analyzer, so `EXPLAIN AST optimize = 1` must decide that from the mode the query
# would actually use. The explained statement's own `SETTINGS` win over the session default: with
# `SETTINGS allow_experimental_analyzer = 0` a real `SELECT ... FROM pv(...)` takes the legacy path and
# succeeds with the base-table grants alone, even when the session enables the analyzer, so `EXPLAIN`
# must not deny it there either.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base_only="base_only_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${base_only};
CREATE TABLE ${CLICKHOUSE_DATABASE}.secret_base (x Int32, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.secret_base VALUES (1, 'a');

CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

CREATE USER ${base_only};
-- No grant on the view object, only on the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${base_only};
"

run() {
    local label="$1"
    local query="$2"
    local out
    # The session enables the analyzer; only the query's own SETTINGS turn it off.
    out=$(${CLICKHOUSE_CLIENT} --user "${base_only}" --enable_analyzer 1 --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -ne 0 ]; then
        if echo "${out}" | grep -q "ACCESS_DENIED"; then
            echo "${label}: ACCESS_DENIED"
        else
            echo "${label}: UNEXPECTED ERROR: ${out}"
        fi
        return
    fi
    if echo "${out}" | grep -q "secret_base"; then
        echo "${label}: OK, view body revealed"
    else
        echo "${label}: OK, view body not revealed"
    fi
}

# The real query in legacy mode does not require a grant on the view object.
run "SELECT      analyzer disabled in query" "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1) SETTINGS allow_experimental_analyzer = 0"
run "EXPLAIN AST analyzer disabled in query" "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1) SETTINGS allow_experimental_analyzer = 0"

# With the session default (analyzer on) both are denied - the reference pins that EXPLAIN follows the query.
run "SELECT      session default          " "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
run "EXPLAIN AST session default          " "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${base_only};
"
