#!/usr/bin/env bash

# The legacy (`allow_experimental_analyzer = 0`) `EXPLAIN SYNTAX` and `EXPLAIN AST optimize = 1` paths run
# the interpreter in `analyze()` mode only, so they never reach `StorageAlias::read`, where the `SELECT`
# grant on an `Alias` table's target is enforced. Without reproducing that check they would format the
# rewritten query - expanding `*` into the target's column names - for a user whose real `SELECT` is denied.
# This is the legacy counterpart of `05048_explain_query_tree_alias_target_table_access`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE TABLE ${db}.target (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${db}.target VALUES (1, 'a');
CREATE TABLE ${db}.alias_t ENGINE = Alias('${db}', 'target');
CREATE USER ${user};
GRANT SELECT ON ${db}.alias_t TO ${user};
"

# Print 'OK' only when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons,
# and the full unexpected error otherwise (which makes the reference diff fail) so that a positive
# case that starts throwing a different exception cannot silently pass.
run() {
    local label="$1"
    local query="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 0 --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

probe() {
    run "  SELECT column"                 "SELECT y FROM ${db}.alias_t"
    run "  EXPLAIN SYNTAX column"         "EXPLAIN SYNTAX SELECT y FROM ${db}.alias_t"
    run "  EXPLAIN AST optimize column"   "EXPLAIN AST optimize = 1 SELECT y FROM ${db}.alias_t"
    run "  SELECT asterisk"               "SELECT * FROM ${db}.alias_t"
    run "  EXPLAIN SYNTAX asterisk"       "EXPLAIN SYNTAX SELECT * FROM ${db}.alias_t"
    run "  EXPLAIN AST optimize asterisk" "EXPLAIN AST optimize = 1 SELECT * FROM ${db}.alias_t"
    run "  SELECT count()"                "SELECT count() FROM ${db}.alias_t"
    run "  EXPLAIN SYNTAX count()"        "EXPLAIN SYNTAX SELECT count() FROM ${db}.alias_t"
    run "  SELECT in subquery"            "SELECT 1 WHERE 1 IN (SELECT y FROM ${db}.alias_t)"
    run "  EXPLAIN SYNTAX in subquery"    "EXPLAIN SYNTAX SELECT 1 WHERE 1 IN (SELECT y FROM ${db}.alias_t)"
}

echo "-- SELECT on the alias only: EXPLAIN must be denied exactly as the real SELECT"
probe

${CLICKHOUSE_CLIENT} --query "GRANT SELECT(y) ON ${db}.target TO ${user}"

echo "-- Column grant on the target: only the reads restricted to that column become allowed"
probe

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.target TO ${user}"

echo "-- Full grant on the target: everything is allowed"
probe

${CLICKHOUSE_CLIENT} --query "
DROP TABLE ${db}.alias_t;
DROP TABLE ${db}.target;
DROP USER ${user};
"
