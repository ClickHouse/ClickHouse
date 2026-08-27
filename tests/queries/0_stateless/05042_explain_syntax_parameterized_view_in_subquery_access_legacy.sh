#!/usr/bin/env bash

# A `SQL SECURITY INVOKER` parameterized view reachable only from a nested subquery -
# `WHERE x IN (SELECT ... FROM pv(...))`, `NOT IN`, `GLOBAL IN`, `EXISTS` - used to leak its
# parameter-substituted body through `EXPLAIN SYNTAX` / `EXPLAIN AST optimize = 1` with the old
# interpreter (`allow_experimental_analyzer = 0`): `ExpandParameterizedViewsMatcher` rewrote the call
# into a plain subquery over the base tables, the outer `InterpreterSelectQuery` analysis never
# resolves `IN` / `EXISTS` operands, and the nested-subquery walk could no longer attribute the
# rewritten subquery to a view. The walk now also runs before the expansion, while the `pv(...)` call
# is still intact, and analyzing the nested `SELECT` then denies exactly as really executing it does.
#
# The `JOIN pv(...) FINAL` / `SAMPLE` shapes (which the matcher deliberately leaves unexpanded) were
# never leaky - `Context::executeTableFunction` resolves every table-expression `pv(...)` under the
# view's SQL security context at name-resolution time - and are pinned here as a regression guard.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

reader="reader_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${reader}, ${full};
-- ReplacingMergeTree so that the FINAL shapes are valid at real execution time.
CREATE TABLE ${CLICKHOUSE_DATABASE}.base1 (y UInt32, z String) ENGINE = ReplacingMergeTree ORDER BY y SAMPLE BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base1 VALUES (1, 'a');

-- A plain table the reader is allowed to read, so that any denial is attributable to the view.
CREATE TABLE ${CLICKHOUSE_DATABASE}.plain (y UInt32) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.plain VALUES (1);

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv1 AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base1 WHERE y = {n:UInt32};
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv_def SQL SECURITY DEFINER AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base1 WHERE y = {n:UInt32};

CREATE USER ${reader}, ${full};
-- The reader can read the plain table and both view objects, but not the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.plain TO ${reader}, ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv1 TO ${reader}, ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_def TO ${reader}, ${full};
-- The full user can additionally read the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base1 TO ${full};
"

# Print 'OK' only when the request succeeds end to end: `curl` must exit 0 and the body must carry
# no exception, so a transport failure or timeout cannot pass as a successful query. Denials print
# 'ACCESS_DENIED' (classified from the body, which ClickHouse fills before the HTTP status), and any
# other failure prints the exit code and the full body, which makes the reference diff fail - a case
# that starts failing differently cannot silently pass.
# The queries go over HTTP: a debug-build client takes seconds just to start, which would push the
# test over the time limit.
run() {
    local user="$1"
    local label="$2"
    local query="$3"
    local out
    local rc
    out=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&enable_analyzer=0" --data-binary "${query}" 2>&1)
    rc=$?
    if echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    elif [ "${rc}" -ne 0 ]; then
        echo "${label}: TRANSPORT ERROR: curl exited with ${rc}: ${out}"
    elif echo "${out}" | grep -q "DB::Exception"; then
        echo "${label}: UNEXPECTED ERROR: ${out}"
    else
        echo "${label}: OK"
    fi
}

# Every shape below must give the reader the same answer for `EXPLAIN` as for the real `SELECT`.
# The five probes of a shape are independent, so they run concurrently: one probe on a loaded
# debug-build server can take many seconds, and serially the total exceeded the time limit.
check() {
    local label="$1"
    local query="$2"
    echo "-- ${label}"
    local dir
    dir=$(mktemp -d "${CLICKHOUSE_TMP}/probes_XXXXXX")
    run "${reader}" "  reader SELECT"                "${query}" > "${dir}/1" &
    run "${reader}" "  reader EXPLAIN SYNTAX"        "EXPLAIN SYNTAX ${query}" > "${dir}/2" &
    run "${reader}" "  reader EXPLAIN AST optimize"  "EXPLAIN AST optimize = 1 ${query}" > "${dir}/3" &
    run "${full}"   "  full SELECT"                  "${query}" > "${dir}/4" &
    run "${full}"   "  full EXPLAIN SYNTAX"          "EXPLAIN SYNTAX ${query}" > "${dir}/5" &
    wait
    cat "${dir}/1" "${dir}/2" "${dir}/3" "${dir}/4" "${dir}/5"
    rm -r "${dir}"
}

db="${CLICKHOUSE_DATABASE}"

check "The parameterized view is inside an IN subquery" \
    "SELECT p.y FROM ${db}.plain AS p WHERE p.y IN (SELECT y FROM ${db}.pv1(n = 1))"

check "The parameterized view is inside a NOT IN subquery" \
    "SELECT p.y FROM ${db}.plain AS p WHERE p.y NOT IN (SELECT y FROM ${db}.pv1(n = 1))"

check "The parameterized view is inside a GLOBAL IN subquery" \
    "SELECT p.y FROM ${db}.plain AS p WHERE p.y GLOBAL IN (SELECT y FROM ${db}.pv1(n = 1))"

check "The parameterized view is inside an EXISTS subquery" \
    "SELECT p.y FROM ${db}.plain AS p WHERE EXISTS (SELECT 1 FROM ${db}.pv1(n = 1))"

check "The parameterized view is inside an IN subquery nested in another IN subquery" \
    "SELECT p.y FROM ${db}.plain AS p WHERE p.y IN (SELECT p2.y FROM ${db}.plain AS p2 WHERE p2.y IN (SELECT y FROM ${db}.pv1(n = 1)))"

# Never-leaky regression guards: a JOIN-side call the expansion deliberately leaves intact
# (FINAL / SAMPLE) is denied at name-resolution time, independently of join position.
check "JOIN-side parameterized view with FINAL" \
    "SELECT p.y FROM ${db}.plain AS p JOIN ${db}.pv1(n = 1) AS v FINAL ON p.y = v.y"

check "JOIN-side parameterized view with SAMPLE" \
    "SELECT p.y FROM ${db}.plain AS p JOIN ${db}.pv1(n = 1) AS v SAMPLE 1/2 ON p.y = v.y"

check "IN subquery over the view with FINAL" \
    "SELECT p.y FROM ${db}.plain AS p WHERE p.y IN (SELECT y FROM ${db}.pv1(n = 1) FINAL)"

# Over-denial controls.
check "SQL SECURITY DEFINER parameterized view inside an IN subquery" \
    "SELECT p.y FROM ${db}.plain AS p WHERE p.y IN (SELECT y FROM ${db}.pv_def(n = 1))"

check "No view at all, IN subquery over a granted table" \
    "SELECT p.y FROM ${db}.plain AS p WHERE p.y IN (SELECT y FROM ${db}.plain)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv1;
DROP VIEW ${CLICKHOUSE_DATABASE}.pv_def;
DROP TABLE ${CLICKHOUSE_DATABASE}.base1;
DROP TABLE ${CLICKHOUSE_DATABASE}.plain;
DROP USER ${reader}, ${full};
"
