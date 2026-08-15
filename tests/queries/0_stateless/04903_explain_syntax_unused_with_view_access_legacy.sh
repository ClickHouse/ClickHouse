#!/usr/bin/env bash

# An unused WITH subquery is not evaluated, so checking access to a view inside
# it must not make EXPLAIN fail for a user who can only select from that view.

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

query="WITH c AS (SELECT a FROM v) SELECT 1"

for settings in "SET enable_analyzer = 0;" "SET enable_analyzer = 1, enable_global_with_statement = 0;" "SET enable_analyzer = 1, enable_global_with_statement = 1;"; do
    run "${settings}" "SELECT" "${query}"
    run "${settings}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${query}"
    run "${settings}" "EXPLAIN AST optimize=1" "EXPLAIN AST optimize = 1 ${query}"
done

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${reader};
"
