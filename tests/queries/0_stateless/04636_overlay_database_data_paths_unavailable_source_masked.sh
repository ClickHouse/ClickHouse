#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libpq (PostgreSQL database engine), which is not built in fast test.
#   no-parallel: creates a PostgreSQL database pointing at an unreachable endpoint. Because
#     `show_remote_databases_in_system_tables` defaults to `true`, the database is visible in
#     `system.tables` and `system.columns`, so any concurrent query that scans those tables
#     without a database filter would try to connect to the unreachable endpoint and fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# The connection errors that the probes produce are logged server-side at error level; keep them
# out of the test's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The data entrypoints through a read-only Overlay facade (SELECT under both analyzers, INSERT,
# CHECK TABLE) must stay fail-closed even when the source database is backed by a remote
# catalog and that catalog is unavailable: resolving the facade name loads the source table, and
# for such sources even the existence probe connects to the remote server and throws its own
# error. Until the source-side grant is proven, that error must not surface through the facade —
# otherwise a user with only facade-side grants could distinguish a hidden broken source from a
# missing name. With facade-only grants every data entrypoint answers `ACCESS_DENIED`; a user who
# also holds the source-side grant sees the source's own connection error, the same as when
# querying the source directly.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_PG="db_pg_${SUF}"
DB_OVL="db_ovl_${SUF}"
DB_LOC="db_loc_${SUF}" # a plain database, to be the left table of a JOIN with the facade
USER_OVL="u_ovl_${SUF}"   # data grants on the facade only
USER_DUAL="u_dual_${SUF}" # data grants on the facade and on the source

# The PostgreSQL database engine does not connect at CREATE time, so an unreachable endpoint is
# fine here. Port 1 on localhost is never listening, so every probe fails instantly with
# "connection refused" instead of hanging.
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_PG};
    DROP DATABASE IF EXISTS ${DB_LOC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_PG} ENGINE = PostgreSQL('127.0.0.1:1', 'fake_db', 'user', 'password');
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_PG}');

    CREATE DATABASE ${DB_LOC};
    CREATE TABLE ${DB_LOC}.l (x UInt64) ENGINE = Memory;
    INSERT INTO ${DB_LOC}.l VALUES (1);
    -- A two-shard Distributed table: only for such a table does the old analyzer run
    -- InJoinSubqueriesPreprocessor, which looks up every table of an IN / JOIN subquery
    -- while rewriting it.
    CREATE TABLE ${DB_LOC}.d (x UInt64) ENGINE = Distributed(test_cluster_two_shards, ${DB_LOC}, l);

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SELECT, INSERT, CHECK ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT SELECT ON ${DB_LOC}.* TO ${USER_OVL};

    GRANT SELECT, INSERT, CHECK ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SELECT, INSERT, CHECK ON ${DB_PG}.* TO ${USER_DUAL};
    GRANT SELECT ON ${DB_LOC}.* TO ${USER_DUAL};
"

echo 'Facade-only grants: SELECT is denied under both analyzers, not the connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_OVL}.t SETTINGS enable_analyzer = 1" 2>&1 | grep -o ACCESS_DENIED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_OVL}.t SETTINGS enable_analyzer = 0" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Facade-only grants: a JOIN with the facade on the right side is denied under both analyzers'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_LOC}.l AS a JOIN ${DB_OVL}.t AS b ON a.x = b.x SETTINGS enable_analyzer = 1" 2>&1 | grep -o ACCESS_DENIED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_LOC}.l AS a JOIN ${DB_OVL}.t AS b ON a.x = b.x SETTINGS enable_analyzer = 0" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Facade-only grants: a bare facade table on the right side of IN is denied under both analyzers'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_LOC}.l WHERE x IN ${DB_OVL}.t SETTINGS enable_analyzer = 1" 2>&1 | grep -o ACCESS_DENIED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_LOC}.l WHERE x IN ${DB_OVL}.t SETTINGS enable_analyzer = 0" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Facade-only grants: a facade table in a subquery of a distributed query is denied under both analyzers'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_LOC}.d WHERE x GLOBAL IN (SELECT x FROM ${DB_OVL}.t) SETTINGS enable_analyzer = 1" 2>&1 | grep -o ACCESS_DENIED | uniq
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_LOC}.d WHERE x GLOBAL IN (SELECT x FROM ${DB_OVL}.t) SETTINGS enable_analyzer = 0" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Facade-only grants: INSERT is denied, not the connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "INSERT INTO ${DB_OVL}.t VALUES (1)" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Facade-only grants: CHECK TABLE is denied, not the connection error'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "CHECK TABLE ${DB_OVL}.t" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Dual grants: the source connection error is visible, the same as on direct access'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT * FROM ${DB_OVL}.t SETTINGS enable_analyzer = 1" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT * FROM ${DB_OVL}.t SETTINGS enable_analyzer = 0" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "INSERT INTO ${DB_OVL}.t VALUES (1)" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT * FROM ${DB_PG}.t" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT * FROM ${DB_LOC}.l AS a JOIN ${DB_OVL}.t AS b ON a.x = b.x SETTINGS enable_analyzer = 1" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT * FROM ${DB_LOC}.l AS a JOIN ${DB_OVL}.t AS b ON a.x = b.x SETTINGS enable_analyzer = 0" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT * FROM ${DB_LOC}.l WHERE x IN ${DB_OVL}.t SETTINGS enable_analyzer = 1" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT * FROM ${DB_LOC}.l WHERE x IN ${DB_OVL}.t SETTINGS enable_analyzer = 0" 2>&1 | grep -o POSTGRESQL_CONNECTION_FAILURE | uniq

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_PG};
    DROP DATABASE ${DB_LOC};
    DROP USER ${USER_OVL};
    DROP USER ${USER_DUAL};
"
