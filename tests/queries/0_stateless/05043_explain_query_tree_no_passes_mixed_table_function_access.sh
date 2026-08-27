#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MySQL integration is not available in the fast test build.

# `EXPLAIN QUERY TREE run_passes = 0` dumps the unresolved tree and must not resolve any table
# function: resolving `mysql(...)` connects to the remote server to fetch the table structure. The
# throwaway resolution that reproduces the planner's `SELECT` access check must therefore stand down
# for any query that mentions a registered table function, even when ordinary tables sit next to it
# (in a sibling JOIN or an IN subquery): query analysis resolves the whole tree, so checking the
# table would resolve the sibling function too. Standing down is fail-safe - the unresolved dump
# reveals nothing beyond the user's own query text - and the check still runs for queries over plain
# tables and over the `view` wrapper, which resolves locally.
#
# Nobody is expected to listen on the MySQL address below: if a regression makes the dump resolve the
# table function again, the connection attempts show up as `mysqlxx` retry noise in the server logs
# streamed to the client, and the probe reports it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="reader_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE TABLE ${db}.granted (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${db}.secret (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE USER ${user};
GRANT SELECT ON ${db}.granted TO ${user};
"

mysql_tf="mysql('127.0.0.1:3306', 'db', 't', 'user', 'password')"

probe() {
    local label="$1"
    local query="$2"
    local out
    local rc
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer=1 --query "EXPLAIN QUERY TREE run_passes = 0 ${query}" 2>&1)
    rc=$?
    if [ "${rc}" -eq 0 ]; then
        if echo "${out}" | grep -q "mysqlxx"; then
            echo "${label}: CONNECTION ATTEMPTED"
        else
            echo "${label}: OK"
        fi
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

# The access check runs for queries of plain tables (and the local `view` wrapper): denied without a
# grant, allowed with one, exactly like the real SELECT.
probe "granted table" "SELECT id FROM ${db}.granted"
probe "table without a grant" "SELECT id FROM ${db}.secret"
probe "table without a grant inside the view wrapper" "SELECT id FROM view(SELECT id FROM ${db}.secret)"

# A registered table function anywhere in the query makes the check stand down, so the dump must
# come back without a single connection attempt - including for the table the user may not read,
# whose unresolved dump reveals nothing.
probe "granted table with the function in an IN subquery" \
    "SELECT id FROM ${db}.granted WHERE id IN (SELECT id FROM ${mysql_tf})"
probe "granted table joined with the function" \
    "SELECT g.id FROM ${db}.granted AS g JOIN ${mysql_tf} AS m ON g.id = m.id"
probe "ungranted table with the function in an IN subquery" \
    "SELECT id FROM ${db}.secret WHERE id IN (SELECT id FROM ${mysql_tf})"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE ${db}.granted;
DROP TABLE ${db}.secret;
DROP USER ${user};
"
