#!/usr/bin/env bash

# `EXPLAIN SYNTAX` deliberately leaves some parameterized view calls unexpanded: `FINAL` / `SAMPLE`
# calls, and views created with `SQL SECURITY DEFINER` or `NONE`. For a statement that only wraps a
# `SELECT` (`INSERT INTO dst SELECT ... FROM pv(...)`) the analyzer dump declines the non-`SELECT` root,
# so the legacy formatting visitor is reached, and that one inlines the view body through
# `StorageView::replaceWithSubquery` on its own. The `SELECT` grant on the view object - which the
# analyzer requires for a real `SELECT ... FROM pv(...)` - therefore has to be enforced for every
# referenced parameterized view, not only for the ones that were expanded.

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

-- Expansion is skipped for a DEFINER view: its body is read under the definer's privileges, so
-- inlining it would re-analyze it under the invoker's context.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv_def
    DEFINER = CURRENT_USER SQL SECURITY DEFINER
    AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

-- Expansion is skipped for a FINAL / SAMPLE call: the modifier is rejected on the resulting subquery.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv_inv AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

CREATE USER ${base_only}, ${full};
-- The base_only user may read the base table and write to the destination, but has no grant on the views.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${base_only};
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dst TO ${base_only};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${full};
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dst TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_def TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_inv TO ${full};
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

shape_names=("DEFINER" "FINAL  ")
shape_calls=("pv_def(n = 1)" "pv_inv(n = 1) FINAL")

for enable_analyzer in 1 0
do
    echo "-- enable_analyzer = ${enable_analyzer}"

    for i in "${!shape_names[@]}"
    do
        name="${shape_names[$i]}"
        statement="INSERT INTO ${CLICKHOUSE_DATABASE}.dst SELECT * FROM ${CLICKHOUSE_DATABASE}.${shape_calls[$i]}"

        # The real statement: the reference below pins that EXPLAIN behaves exactly the same way.
        run "${base_only}" "${name} INSERT SELECT         base_only" "${statement}"
        run "${full}"      "${name} INSERT SELECT         full     " "${statement}"

        run "${base_only}" "${name} EXPLAIN SYNTAX INSERT base_only" "EXPLAIN SYNTAX ${statement}"
        run "${full}"      "${name} EXPLAIN SYNTAX INSERT full     " "EXPLAIN SYNTAX ${statement}"

        run "${base_only}" "${name} EXPLAIN AST    INSERT base_only" "EXPLAIN AST optimize = 1 ${statement}"
        run "${full}"      "${name} EXPLAIN AST    INSERT full     " "EXPLAIN AST optimize = 1 ${statement}"
    done
done

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv_def;
DROP VIEW ${CLICKHOUSE_DATABASE}.pv_inv;
DROP TABLE ${CLICKHOUSE_DATABASE}.dst;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${base_only}, ${full};
"
