#!/usr/bin/env bash
# Test for https://github.com/ClickHouse/ClickHouse/issues/26746
# A `CREATE TABLE ... AS SELECT` that is denied because the user lacks SELECT on a table referenced
# by a subquery must not leave an empty orphan table behind. Before the fix the table was created
# first and the access check happened only during the populating INSERT SELECT, so the access error
# left the table in place and a retry reported `TABLE_ALREADY_EXISTS` instead of the access error.
#
# Both forms are checked: the column list inferred from the SELECT, and an explicit column list. The
# explicit form takes a different code path (`create.columns_list`) that never analyzes the SELECT, so
# it needs the same up-front access check; otherwise the orphan table is still left behind.

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

# The fix lives in the analyzer code path, so force it on regardless of the test profile.
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

echo "-- once column-level SELECT(y) on t2 is granted, both queries succeed and populate the table:"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(y) ON ${CLICKHOUSE_DATABASE}.t2 TO ${user}"
"${client[@]}" --query "${inferred_query}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dst"
"${client[@]}" --query "${explicit_query}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dst_explicit"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE dst;
DROP TABLE dst_explicit;
DROP TABLE t0;
DROP TABLE t1;
DROP TABLE t2;
DROP USER ${user};
"
