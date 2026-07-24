#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libmysqlclient (MySQL database engine), which is not built in fast test.
#   no-parallel: attaches a MySQL database pointing at an unreachable endpoint. Because
#     `show_remote_databases_in_system_tables` defaults to `true`, the database is visible in
#     `system.tables` and `system.columns`, so any concurrent query that scans those tables
#     without a database filter would try to connect to the unreachable endpoint and fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# The connection errors that the listing probes produce are logged server-side at error level;
# keep them out of the test's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Listing-style metadata readers through a read-only Overlay facade (`SHOW TABLES` /
# `system.tables`, `system.columns`, `system.data_skipping_indices`, ...) must stay fail-closed
# when a source database is backed by a remote catalog and that catalog is unavailable: opening
# the source's tables iterator (`DatabaseMySQL` eagerly refreshes from `INFORMATION_SCHEMA`)
# connects to the remote server and throws its own error before any source-side grant is proven.
# Until the caller proves `SHOW TABLES` on the source database, that error must not surface
# through the facade — otherwise a user with only facade-side grants could distinguish a hidden
# broken source from an empty or denied one. With facade-only grants every listing simply shows
# no tables for the facade; a user who also holds the source-side grant sees the source's own
# connection error, the same as when listing the source directly.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_MY="db_my_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_OVL="u_ovl_${SUF}"   # SHOW granted on the facade only
USER_DUAL="u_dual_${SUF}" # SHOW granted on the facade and on the source

# `CREATE DATABASE ... ENGINE = MySQL` validates the connection eagerly, so ATTACH is used to
# register a source whose endpoint is unreachable, modelling a source that went down after it
# was attached. Port 1 on localhost is never listening, so every probe fails instantly with
# "connection refused" instead of hanging.
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_MY};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    ATTACH DATABASE ${DB_MY} ENGINE = MySQL('127.0.0.1:1', 'fake_db', 'user', 'password');
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_MY}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW ON ${DB_MY}.* TO ${USER_DUAL};
"

echo 'Facade-only grants: SHOW TABLES through the facade is empty, not the connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW TABLES FROM ${DB_OVL}"
echo "rc=$?"

echo 'Facade-only grants: system.tables shows no tables for the facade, not the connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() FROM system.tables WHERE database = '${DB_OVL}'"

echo 'Facade-only grants: system.columns shows no columns for the facade, not the connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() FROM system.columns WHERE database = '${DB_OVL}'"

echo 'Dual grants: the source connection error is visible, the same as on direct listing'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SHOW TABLES FROM ${DB_OVL}" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SHOW TABLES FROM ${DB_MY}" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_MY};
    DROP USER ${USER_OVL};
    DROP USER ${USER_DUAL};
"
