#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libpq (PostgreSQL database engine), which is not built in fast test.
#   no-parallel: creates a PostgreSQL database pointing at an unreachable endpoint. Because
#     `show_remote_databases_in_system_tables` defaults to `true`, the database is visible in
#     `system.tables` and `system.columns`, so any concurrent query that scans those tables
#     without a database filter would try to connect to the unreachable endpoint and fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Metadata queries through a read-only Overlay facade must stay fail-closed even when the source
# database is backed by a remote catalog and that catalog is unavailable: for such sources even the
# metadata-only existence probe connects to the remote server and throws its own error. Until the
# source-side grant is proven, that error must not surface through the facade — otherwise a user
# with only the facade-side grant could distinguish a hidden broken source from a missing name.
# With the facade-only grant the answers stay `0` for EXISTS and `ACCESS_DENIED` for
# DESCRIBE / SHOW CREATE; a user who also holds the source-side grant sees the source's own
# connection error, the same as when querying the source directly.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_PG="db_pg_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_OVL="u_ovl_${SUF}"   # SHOW on the facade only
USER_DUAL="u_dual_${SUF}" # SHOW on the facade and on the source

# The connection errors that the probes produce are logged server-side at error level; keep them
# out of the test's stderr.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level=fatal"

# The PostgreSQL database engine does not connect at CREATE time, so an unreachable endpoint is
# fine here. Port 1 on localhost is never listening, so every probe fails instantly with
# "connection refused" instead of hanging.
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_PG};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_PG} ENGINE = PostgreSQL('127.0.0.1:1', 'fake_db', 'user', 'password');
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_PG}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW ON ${DB_PG}.* TO ${USER_DUAL};
"

echo 'Facade-only SHOW grant: EXISTS through the facade answers 0, not the connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS TABLE ${DB_OVL}.t"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS VIEW ${DB_OVL}.v"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS DICTIONARY ${DB_OVL}.d"

echo 'Facade-only SHOW grant: DESCRIBE and SHOW CREATE are denied, not the connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "DESCRIBE TABLE ${DB_OVL}.t" 2>&1 | grep -o ACCESS_DENIED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW CREATE TABLE ${DB_OVL}.t" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Dual SHOW grants: the source connection error is visible, the same as on direct access'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "EXISTS TABLE ${DB_OVL}.t" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "DESCRIBE TABLE ${DB_OVL}.t" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_PG};
    DROP USER ${USER_OVL};
    DROP USER ${USER_DUAL};
"
