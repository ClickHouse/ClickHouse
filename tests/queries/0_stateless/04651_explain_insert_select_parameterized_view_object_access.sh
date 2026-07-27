#!/usr/bin/env bash

# A parameterized view can be called from a `SELECT` nested inside another statement, e.g.
# `EXPLAIN SYNTAX INSERT INTO dst SELECT ... FROM pv(...)`. `EXPLAIN` expands the view call in those
# nested `SELECT`s too, so the access check that enforces the `SELECT` grant on the view object itself
# has to descend into the wrapping statement instead of giving up because the explained query is not a
# top-level `SELECT`. Otherwise a user who may read the base table but has no grant on the view could
# read the view definition through the wrapper while the real `INSERT ... SELECT` is denied.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base_only="base_only_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${base_only}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.secret_base (x Int32, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.secret_base VALUES (1, 'a');
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst (x Int32, y String) ENGINE = MergeTree ORDER BY x;

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

CREATE USER ${base_only}, ${full};
-- The base_only user may read the base table and write to the destination, but has no grant on the view.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${base_only};
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dst TO ${base_only};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${full};
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dst TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv TO ${full};
"

# Reports whether the query succeeded and whether its output revealed the view's inner query
# (the base table name only appears in the dump if the view body was inlined into it).
run() {
    local user="$1"
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer "${enable_analyzer}" --query "${query}" 2>&1)
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

for enable_analyzer in 1 0
do
    echo "-- enable_analyzer = ${enable_analyzer}"

    # The real statement: the reference below pins that EXPLAIN behaves exactly the same way.
    run "${base_only}" "INSERT SELECT         base_only" "INSERT INTO ${CLICKHOUSE_DATABASE}.dst SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${full}"      "INSERT SELECT         full     " "INSERT INTO ${CLICKHOUSE_DATABASE}.dst SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

    run "${base_only}" "EXPLAIN SYNTAX INSERT base_only" "EXPLAIN SYNTAX INSERT INTO ${CLICKHOUSE_DATABASE}.dst SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${full}"      "EXPLAIN SYNTAX INSERT full     " "EXPLAIN SYNTAX INSERT INTO ${CLICKHOUSE_DATABASE}.dst SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

    run "${base_only}" "EXPLAIN AST    INSERT base_only" "EXPLAIN AST optimize = 1 INSERT INTO ${CLICKHOUSE_DATABASE}.dst SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${full}"      "EXPLAIN AST    INSERT full     " "EXPLAIN AST optimize = 1 INSERT INTO ${CLICKHOUSE_DATABASE}.dst SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
done

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP TABLE ${CLICKHOUSE_DATABASE}.dst;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${base_only}, ${full};
"
