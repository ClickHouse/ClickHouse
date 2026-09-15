#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libmysqlclient (MySQL database engine), which is not built in fast test.
#   no-parallel: attaches a MySQL database pointing at an unreachable endpoint. Because
#     `show_remote_databases_in_system_tables` defaults to `true`, the database is visible in
#     `system.tables` and `system.columns`, so any concurrent query that scans those tables
#     without a database filter would try to connect to the unreachable endpoint and fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.columns` must prove the dual grant (facade + underlying source) *before* it locks the
# table and probes its metadata. For a `Merge` table the metadata and the size / serialization-hint
# probes enumerate the tables of the database it covers; over an unavailable MySQL database that
# enumeration throws the connection error. A user who holds only the facade-side grant must get an
# empty result, not that error - otherwise the facade would reveal both the hidden table and the
# state of a source the user has no grant on. A user who also holds the source-side grant sees the
# same connection error as on direct access.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_MY="db_my_${SUF}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_OVL="u_ovl_${SUF}"   # SHOW on the facade only
USER_DUAL="u_dual_${SUF}" # SHOW on the facade and on the source

# The connection errors that the probes produce are logged server-side at error level; keep them
# out of the test's stderr.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level=fatal"

# `CREATE TABLE ... ENGINE = Merge` enumerates the covered database eagerly, so the `Merge` table is
# created over an ordinary database of that name first, which is then replaced by the unreachable
# MySQL one (`CREATE DATABASE ... ENGINE = MySQL` validates the connection eagerly, so ATTACH is
# used). The `Merge` table resolves the database by name on every use. Port 1 on localhost is never
# listening, so every probe fails instantly with "connection refused" instead of hanging.
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP DATABASE IF EXISTS ${DB_MY};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_MY} ENGINE = Atomic;
    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.t_merge (id UInt32, s String) ENGINE = Merge('${DB_MY}', '.*');
    DROP DATABASE ${DB_MY};
    ATTACH DATABASE ${DB_MY} ENGINE = MySQL('127.0.0.1:1', 'fake_db', 'user', 'password');
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW ON ${DB_SRC}.* TO ${USER_DUAL};
"

echo 'Sanity: the metadata probe of the Merge table over the unavailable source fails on direct access'
${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.columns WHERE database = '${DB_SRC}' AND table = 't_merge'
" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq

echo 'Facade-only SHOW grant: listing the columns through the facade yields no rows and no connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "
    SELECT name FROM system.columns WHERE database = '${DB_OVL}' AND table = 't_merge'
" 2>&1 | wc -l

echo 'Facade-only SHOW grant: the size columns through the facade yield no rows and no connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "
    SELECT name, data_compressed_bytes FROM system.columns WHERE database = '${DB_OVL}' AND table = 't_merge'
" 2>&1 | wc -l

echo 'Facade-only SHOW grant: the serialization hint column through the facade yields no rows and no connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "
    SELECT name, serialization_hint FROM system.columns WHERE database = '${DB_OVL}' AND table = 't_merge'
" 2>&1 | wc -l

echo 'Dual SHOW grants: the connection error is visible through the facade, the same as on direct access'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "
    SELECT name FROM system.columns WHERE database = '${DB_OVL}' AND table = 't_merge'
" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "
    SELECT name, data_compressed_bytes FROM system.columns WHERE database = '${DB_OVL}' AND table = 't_merge'
" 2>&1 | grep -o ALL_CONNECTION_TRIES_FAILED | uniq

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_SRC};
    DROP DATABASE ${DB_MY};
    DROP USER ${USER_OVL};
    DROP USER ${USER_DUAL};
"
