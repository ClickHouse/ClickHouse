#!/usr/bin/env bash
# Test for https://github.com/ClickHouse/ClickHouse/issues/26746
# A `CREATE TABLE ... AS SELECT` that is denied because the user lacks SELECT on a table referenced
# by a subquery must not leave an empty orphan table behind. Before the fix the table was created
# first and the access check happened only during the populating INSERT SELECT, so the access error
# left the table in place and a retry reported `TABLE_ALREADY_EXISTS` instead of the access error.
#
# The fix (for Atomic databases) creates the table under a temporary name, runs the populating
# INSERT SELECT into it, and only then atomically publishes it under the final name with a RENAME. If
# the SELECT is denied (or fails for any other reason) the temporary table is dropped, so no orphan is
# left behind and the access check is done by the real INSERT SELECT (column-aware, and covering IN /
# scalar subqueries). Both forms are checked: the column list inferred from the SELECT, and an explicit
# column list -- both route through the same temporary-table path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# t2 has an extra, unused column so that the success path below can grant access to only the
# referenced column (y). This proves the up-front check is column-aware and would reject the
# old whole-table SELECT pre-check, which would deny a valid column-level grant.
${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE TABLE t0 (y Int) ENGINE = Memory;
CREATE TABLE t1 (y Int) ENGINE = Memory;
CREATE TABLE t2 (y Int, z Int) ENGINE = Memory;
INSERT INTO t0 VALUES (1), (2), (3);
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (1, 10);
CREATE USER ${user} IDENTIFIED WITH plaintext_password BY '${user}';
GRANT TABLE ENGINE ON Memory TO ${user};
GRANT CREATE TABLE, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t0 TO ${user};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user};
"

# The denied table t2 is referenced only from a nested WHERE-IN subquery, never in a FROM position.
inferred_query="CREATE TABLE dst ENGINE = Memory AS SELECT * FROM t0 WHERE y IN (SELECT y FROM t1 WHERE y IN (SELECT y FROM t2 WHERE y < 2))"
# Same SELECT, but the destination columns are given explicitly. This goes through the column-list code path.
explicit_query="CREATE TABLE dst_explicit (y Int) ENGINE = Memory AS SELECT y FROM t0 WHERE y IN (SELECT y FROM t1 WHERE y IN (SELECT y FROM t2 WHERE y < 2))"

# The temporary-table fix is analyzer-independent, but force the analyzer on for a deterministic profile.
client=(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --user "${user}" --password "${user}")

check_denied()
{
    local label="$1" query="$2" table="$3"
    echo "-- [${label}] denied because of missing SELECT on t2 (referenced in a WHERE-IN subquery):"
    "${client[@]}" --query "${query}" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
    echo "-- [${label}] the denied query must not leave an orphan table:"
    ${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${table}"
    echo "-- [${label}] a retry must still be ACCESS_DENIED, not TABLE_ALREADY_EXISTS:"
    "${client[@]}" --query "${query}" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
}

check_denied "inferred columns" "${inferred_query}" "dst"
check_denied "explicit columns" "${explicit_query}" "dst_explicit"

# `CREATE TABLE IF NOT EXISTS dst ... AS SELECT` for a table that already exists is a no-op: the
# temporary-table path sees the target already exists and returns early, so the populating INSERT SELECT
# never runs, the SELECT is not executed and access to t2 must not be required. The statement must
# succeed and leave the existing table untouched, not raise ACCESS_DENIED for the no-op.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE dst_noop (y Int) ENGINE = Memory; INSERT INTO dst_noop VALUES (42);"
# Capture both the exit status and the output. Asserting the exit status is 0 (not merely that the
# output has no ACCESS_DENIED) is what proves the no-op succeeds: any other failure -- e.g.
# TABLE_ALREADY_EXISTS or an analysis error -- also produces empty `grep` output and would otherwise
# pass silently. The ACCESS_DENIED check is kept as a separate assertion.
noop_output=$("${client[@]}" --query "CREATE TABLE IF NOT EXISTS dst_noop (y Int) ENGINE = Memory AS SELECT y FROM t0 WHERE y IN (SELECT y FROM t2 WHERE y < 2)" 2>&1)
noop_status=$?
echo "-- [if not exists, table exists] no-op must succeed even though t2 is denied:"
if [ "${noop_status}" -eq 0 ]; then echo "succeeded"; else echo "FAILED (exit ${noop_status}): ${noop_output}"; fi
echo "-- [if not exists, table exists] and the no-op must not raise ACCESS_DENIED:"
echo -n "${noop_output}" | grep -Fo "ACCESS_DENIED" | uniq
echo "-- [if not exists, table exists] the existing table must be left unchanged:"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dst_noop"

# But IF NOT EXISTS for a table that does not yet exist must still be denied (the orphan-table fix still
# applies in that case), and must not leave an empty table behind.
echo "-- [if not exists, table missing] still denied because of missing SELECT on t2:"
"${client[@]}" --query "CREATE TABLE IF NOT EXISTS dst_ine (y Int) ENGINE = Memory AS SELECT y FROM t0 WHERE y IN (SELECT y FROM t2 WHERE y < 2)" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq
echo "-- [if not exists, table missing] the denied query must not leave an orphan table:"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE dst_ine"

echo "-- once column-level SELECT(y) on t2 is granted, both queries succeed and populate the table:"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(y) ON ${CLICKHOUSE_DATABASE}.t2 TO ${user}"
"${client[@]}" --query "${inferred_query}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dst"
"${client[@]}" --query "${explicit_query}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dst_explicit"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE dst;
DROP TABLE dst_explicit;
DROP TABLE dst_noop;
DROP TABLE t0;
DROP TABLE t1;
DROP TABLE t2;
DROP USER ${user};
"
