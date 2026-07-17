#!/usr/bin/env bash

# `EXPLAIN SYNTAX` expands a parameterized view call into its parameter-substituted inner query and,
# because the expanded query no longer contains the view object, runs the access check on the original
# query instead. That pre-check deliberately skips itself when the original query cannot be resolved
# (a "format but do not resolve" shape, e.g. one referencing an unknown table): there is no resolved
# metadata to protect and a real query fails with the same resolution error. But the dump of the
# *expanded* query does reveal the view's inner query, so with the check skipped it used to leak the
# parameter-substituted view body to a user with no `SELECT` grant on either the view or its base
# table. Now `EXPLAIN SYNTAX` falls back to formatting the original, unexpanded query whenever the
# access check could not actually run.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

outsider="outsider_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${outsider}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.secret_base (x Int32, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.secret_base VALUES (1, 'a');

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

CREATE USER ${outsider}, ${full};
-- The outsider has no grants at all. The full user can read both the view and its base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${full};
"

# Reports whether the query succeeded and whether its output revealed the view's inner query
# (the base table name only appears in the dump if the view body was expanded into it).
run() {
    local user="$1"
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "${query}" 2>&1)
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

# The unknown table makes the query unresolvable before the parameterized view is even reached, so the
# access pre-check cannot run and EXPLAIN SYNTAX must fall back to formatting the unexpanded query for
# everyone. (A real SELECT fails with the same UNKNOWN_TABLE error for everyone, revealing nothing.)
unresolvable="EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.nonexistent_table, ${CLICKHOUSE_DATABASE}.pv(n = 1)"

# Here resolution reaches the view first: resolving an INVOKER parameterized view computes its header
# under the invoker's own privileges, so the outsider is denied during the pre-check itself, while for
# the full user the unknown identifier then makes the pre-check skip and the dump falls back.
unresolvable_projection="EXPLAIN SYNTAX SELECT unknown_identifier FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

resolvable="EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

echo "-- Unresolvable query (fails before the view): the access check cannot run, so the view body must not be revealed to anyone"
run "${outsider}" "outsider" "${unresolvable}"
run "${full}"     "full"     "${unresolvable}"

echo "-- Unresolvable projection: the outsider is denied while resolving the INVOKER view itself; the full user gets the unexpanded fallback"
run "${outsider}" "outsider" "${unresolvable_projection}"
run "${full}"     "full"     "${unresolvable_projection}"

echo "-- Resolvable query: the access check runs; the view body is revealed exactly to those who may read the view"
run "${outsider}" "outsider" "${resolvable}"
run "${full}"     "full"     "${resolvable}"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${outsider}, ${full};
"
