#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read-only Overlay facade must not widen access through CREATE paths that resolve the facade
# to its underlying source table:
#   - CREATE TABLE ... AS ov.t copies the source schema (columns, keys, indices, projections),
#     so it needs SHOW COLUMNS on *both* the facade and the source, not just the facade.
#   - CREATE MATERIALIZED VIEW ... TO ov.t writes into the underlying source table at runtime,
#     so it needs SELECT/INSERT on *both* the facade and the source, not just the facade.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_SRC="db_src_${SUF}"
DB_OVL="dbovl_${SUF}"
T="t_x"

USER_OVL="u_ovl_${SUF}"   # grants on the facade only
USER_DUAL="u_dual_${SUF}" # grants on the facade and on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.${T} (d Date, x UInt32, s String) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${DB_SRC}.${T} VALUES ('2000-01-01', 1, 'a');

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    -- USER_OVL: everything needed to CREATE a table and a materialized view in its own database,
    -- plus facade-only grants on the source table it tries to reach through the facade.
    GRANT CREATE TABLE, CREATE VIEW, DROP TABLE, INSERT, SELECT ON ${DB_SRC}.* TO ${USER_OVL};
    GRANT SHOW COLUMNS, SELECT, INSERT ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT CREATE TABLE, CREATE VIEW, DROP TABLE, INSERT, SELECT ON ${DB_SRC}.* TO ${USER_DUAL};
    GRANT SHOW COLUMNS, SELECT, INSERT ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW COLUMNS, SELECT, INSERT ON ${DB_SRC}.* TO ${USER_DUAL};
"

echo 'SHOW COLUMNS on the Overlay database alone is not enough for CREATE TABLE AS through the facade'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "CREATE TABLE ${DB_SRC}.as_ovl_fail AS ${DB_OVL}.${T}" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'SHOW COLUMNS on both the Overlay and the underlying database succeeds'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "CREATE TABLE ${DB_SRC}.as_ovl_ok AS ${DB_OVL}.${T}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.columns WHERE database = '${DB_SRC}' AND table = 'as_ovl_ok'"

echo 'INSERT on the Overlay database alone is not enough for CREATE MATERIALIZED VIEW TO the facade'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "CREATE MATERIALIZED VIEW ${DB_SRC}.mv_fail TO ${DB_OVL}.${T} AS SELECT d, x, s FROM ${DB_SRC}.${T}" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'INSERT on both the Overlay and the underlying database succeeds'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "CREATE MATERIALIZED VIEW ${DB_SRC}.mv_ok TO ${DB_OVL}.${T} AS SELECT d, x, s FROM ${DB_SRC}.${T}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${DB_SRC}' AND name = 'mv_ok'"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
