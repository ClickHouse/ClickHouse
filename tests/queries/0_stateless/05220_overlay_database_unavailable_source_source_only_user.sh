#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libmysqlclient (MySQL database engine), which is not built in fast test.
#   no-parallel: attaches a MySQL database pointing at an unreachable endpoint. Because
#     `show_remote_databases_in_system_tables` defaults to `true`, the database is visible in
#     `system.tables` and `system.columns`, so any concurrent query that scans those tables
#     without a database filter would try to connect to the unreachable endpoint and fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# The connection errors that the probes produce are logged server-side at error level; keep them
# out of the test's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reaching a name through a read-only Overlay facade requires the grant on the facade name as
# written AND on the source name it resolves to. The fail-closed prechecks of the data, metadata,
# parameterized-view and listing entrypoints must therefore stay closed for a user who holds only
# the SOURCE-side grants: when the source is a remote catalog that is unavailable, such a user
# must not see the source's own connection error through the facade (which would tell them which
# source the facade name resolves to) but the same denial as for a hidden healthy source. Only a
# user holding both grants sees the source's own error, the same as on direct access.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_MY="db_my_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_SRC="u_src_${SUF}"   # grants on the source only
USER_DUAL="u_dual_${SUF}" # grants on the facade and on the source

# `CREATE DATABASE ... ENGINE = MySQL` validates the connection eagerly, so ATTACH is used to
# register a source whose endpoint is unreachable, modelling a source that went down after it
# was attached. Port 1 on localhost is never listening, so every probe fails instantly with
# "connection refused" instead of hanging.
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_MY};
    DROP USER IF EXISTS ${USER_SRC};
    DROP USER IF EXISTS ${USER_DUAL};

    ATTACH DATABASE ${DB_MY} ENGINE = MySQL('127.0.0.1:1', 'fake_db', 'user', 'password');
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_MY}');

    CREATE USER ${USER_SRC} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SELECT, INSERT, CHECK, SHOW, CREATE TABLE ON ${DB_MY}.* TO ${USER_SRC};
    GRANT SELECT ON system.tables TO ${USER_SRC};
    GRANT SELECT ON system.columns TO ${USER_SRC};

    GRANT SELECT, INSERT, CHECK, SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SELECT, INSERT, CHECK, SHOW ON ${DB_MY}.* TO ${USER_DUAL};
"

# Report what the source-only user gets: the denial, the source's own error, a leak of the source
# name, the unexpanded parameterized-view call, or something else. The client echoes the query
# text after an error, and the query itself may name the source database (as the target of
# `CREATE TABLE ... AS`), so that line is not part of the server's answer.
function try
{
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${USER_SRC}" --query "$1" 2>&1 | grep -v '^(query: ')
    if echo "${out}" | grep -q "${DB_MY}"; then
        echo "source name leaked"
    elif echo "${out}" | grep -q 'ALL_CONNECTION_TRIES_FAILED'; then
        echo "source error leaked"
    elif echo "${out}" | grep -q 'ACCESS_DENIED'; then
        echo "ACCESS_DENIED"
    elif echo "${out}" | grep -q 'UNKNOWN_FUNCTION'; then
        echo "UNKNOWN_FUNCTION"
    elif echo "${out}" | grep -qF "${DB_OVL}.v"; then
        echo "not inlined"
    else
        echo "${out}"
    fi
}

echo 'Source-only grants: the data entrypoints are denied on the facade, not the connection error'
try "SELECT * FROM ${DB_OVL}.t"
try "INSERT INTO ${DB_OVL}.t VALUES (1)"
try "CHECK TABLE ${DB_OVL}.t"
try "CREATE TABLE ${DB_MY}.t_copy AS ${DB_OVL}.t"

echo 'Source-only grants: the metadata entrypoints are denied on the facade, not the connection error'
try "DESCRIBE TABLE ${DB_OVL}.t"
try "SHOW CREATE TABLE ${DB_OVL}.t"
try "EXISTS TABLE ${DB_OVL}.t"

echo 'Source-only grants: a parameterized view through the facade is indistinguishable from a missing one'
try "SELECT * FROM ${DB_OVL}.v(min = 0)"
try "EXPLAIN SYNTAX SELECT * FROM ${DB_OVL}.v(min = 0)"

echo 'Source-only grants: the listings through the facade are empty, not the connection error'
try "SHOW TABLES FROM ${DB_OVL}"
try "SELECT count() FROM system.tables WHERE database = '${DB_OVL}'"
try "SELECT count() FROM system.columns WHERE database = '${DB_OVL}'"

echo 'Source-only grants: the source itself still reports its own error on direct access'
${CLICKHOUSE_CLIENT} --user="${USER_SRC}" --query "SELECT * FROM ${DB_MY}.t" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_SRC}" --query "SHOW TABLES FROM ${DB_MY}" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq

echo 'Dual grants: the source connection error is visible through the facade'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT * FROM ${DB_OVL}.t" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SHOW TABLES FROM ${DB_OVL}" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_MY};
    DROP USER ${USER_SRC};
    DROP USER ${USER_DUAL};
"
