#!/usr/bin/env bash

# Companion of 04610_explain_syntax_join_view_access_legacy for queries joining MORE THAN TWO tables,
# with the old interpreter (allow_experimental_analyzer = 0).
#
# `InterpreterSelectQuery` runs `JoinedTables::rewriteMultipleJoins` for every query joining more than
# two tables, replacing each top-level table expression with a generated per-table subquery in place.
# By the time the base-table access check runs, no table expression names a table any more, so no view
# is found at all - neither a JOIN-side one nor the leftmost one - and `EXPLAIN SYNTAX` /
# `EXPLAIN AST optimize = 1` dumped the query even though the real `SELECT` through a
# `SQL SECURITY INVOKER` view is denied on its base table. The check now descends into the generated
# subqueries, at the top level and inside the nested-subquery walk alike.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

reader="reader_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${reader}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base1 (y Int32, z String) ENGINE = MergeTree ORDER BY y;
CREATE TABLE ${CLICKHOUSE_DATABASE}.base2 (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base1 VALUES (1, 'a');
INSERT INTO ${CLICKHOUSE_DATABASE}.base2 VALUES (1, 'b');

-- Plain tables the reader is allowed to read, so that any denial is attributable to the views.
CREATE TABLE ${CLICKHOUSE_DATABASE}.plain (y Int32) ENGINE = MergeTree ORDER BY y;
CREATE TABLE ${CLICKHOUSE_DATABASE}.other (y Int32) ENGINE = MergeTree ORDER BY y;
CREATE TABLE ${CLICKHOUSE_DATABASE}.third (y Int32) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.plain VALUES (1);
INSERT INTO ${CLICKHOUSE_DATABASE}.other VALUES (1);
INSERT INTO ${CLICKHOUSE_DATABASE}.third VALUES (1);

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v1 AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base1;
CREATE VIEW ${CLICKHOUSE_DATABASE}.v2 AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base2;

CREATE USER ${reader}, ${full};
-- The reader can read every plain table and both view objects, but neither base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.plain TO ${reader}, ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.other TO ${reader}, ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.third TO ${reader}, ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v1 TO ${reader}, ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v2 TO ${reader}, ${full};
-- The full user can additionally read both base tables.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base1 TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base2 TO ${full};
"

# Print 'OK' only when the request succeeds end to end: `curl` must exit 0 and the body must carry
# no exception, so a transport failure or timeout cannot pass as a successful query. Denials print
# 'ACCESS_DENIED' (classified from the body, which ClickHouse fills before the HTTP status), and any
# other failure prints the exit code and the full body, which makes the reference diff fail - a case
# that starts failing differently cannot silently pass.
# The queries go over HTTP: a debug-build client takes seconds just to start, and with this many
# probes the per-invocation startup cost alone pushed the test over the 180 s limit.
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

check "Three tables, views on the second and third join positions" \
    "SELECT o.y FROM ${db}.other AS o JOIN ${db}.v1 AS a ON o.y = a.y JOIN ${db}.v2 AS b ON o.y = b.y"

check "Three tables, the view is leftmost" \
    "SELECT v.y FROM ${db}.v1 AS v JOIN ${db}.plain AS p ON v.y = p.y JOIN ${db}.other AS t ON v.y = t.y"

check "Three tables joined with commas" \
    "SELECT o.y FROM ${db}.other AS o, ${db}.plain AS p, ${db}.v1 AS v WHERE o.y = p.y AND o.y = v.y"

check "Four tables" \
    "SELECT o.y FROM ${db}.other AS o JOIN ${db}.plain AS p ON o.y = p.y JOIN ${db}.third AS t ON o.y = t.y JOIN ${db}.v1 AS v ON o.y = v.y"

check "The multiple join is inside a nested subquery" \
    "SELECT o.y FROM ${db}.other AS o WHERE o.y IN (SELECT v.y FROM ${db}.plain AS p JOIN ${db}.v1 AS v ON p.y = v.y JOIN ${db}.third AS t ON p.y = t.y)"

# Over-denial controls: without a view, more than two tables must stay allowed for the reader.
check "Three tables, no view at all" \
    "SELECT o.y FROM ${db}.other AS o JOIN ${db}.plain AS p ON o.y = p.y JOIN ${db}.third AS t ON o.y = t.y"

check "Nested three-table join, no view at all" \
    "SELECT o.y FROM ${db}.other AS o WHERE o.y IN (SELECT p.y FROM ${db}.plain AS p JOIN ${db}.third AS t ON p.y = t.y JOIN ${db}.other AS u ON p.y = u.y)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v1;
DROP VIEW ${CLICKHOUSE_DATABASE}.v2;
DROP TABLE ${CLICKHOUSE_DATABASE}.base1;
DROP TABLE ${CLICKHOUSE_DATABASE}.base2;
DROP TABLE ${CLICKHOUSE_DATABASE}.plain;
DROP TABLE ${CLICKHOUSE_DATABASE}.other;
DROP TABLE ${CLICKHOUSE_DATABASE}.third;
DROP USER ${reader}, ${full};
"
