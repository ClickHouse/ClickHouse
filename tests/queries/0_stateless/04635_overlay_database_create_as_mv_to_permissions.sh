#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Create-time paths through a read-only Overlay facade follow the same dual-grant contract as the
# regular read/write paths — the facade must not widen access:
#   - CREATE TABLE ... AS ov.t (and CLONE AS ov.t) copies the schema of the underlying source
#     table, so it requires SHOW COLUMNS on both the facade name and the source table.
#   - CREATE MATERIALIZED VIEW ... TO ov.t funnels writes into the underlying source table (a
#     plain materialized view re-checks no target grant at insert time), so creating it requires
#     SELECT and INSERT on both the facade name and the source table.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_SRC="db_src_${SUF}"
DB_OVL="dbovl_${SUF}"
DB_DST="db_dst_${SUF}"
T="t_ca"

USER_OVL="u_ca_ovl_${SUF}"   # grants on the facade only
USER_DUAL="u_ca_dual_${SUF}" # grants on the facade and on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_DST};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.${T} (d Date, x UInt32, s String) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${DB_SRC}.${T} VALUES ('2000-01-01', 1, 'a');

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
    CREATE DATABASE ${DB_DST} ENGINE = Atomic;
    CREATE TABLE ${DB_DST}.feeder (d Date, x UInt32, s String) ENGINE = MergeTree ORDER BY x;

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW ON ${DB_DST}.* TO ${USER_OVL};
    GRANT SELECT, INSERT ON ${DB_DST}.* TO ${USER_OVL};
    GRANT SHOW COLUMNS ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT SELECT, INSERT ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW ON ${DB_DST}.* TO ${USER_DUAL};
    GRANT SELECT, INSERT ON ${DB_DST}.* TO ${USER_DUAL};
    GRANT SHOW COLUMNS ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SELECT, INSERT ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW COLUMNS ON ${DB_SRC}.* TO ${USER_DUAL};
    GRANT SELECT, INSERT ON ${DB_SRC}.* TO ${USER_DUAL};
"

echo 'SHOW COLUMNS on the Overlay alone is not enough for CREATE TABLE ... AS through the facade'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "CREATE TABLE ${DB_DST}.copy1 AS ${DB_OVL}.${T}" 2>&1 | grep -o ACCESS_DENIED | uniq

echo '... nor with an explicit ENGINE'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "CREATE TABLE ${DB_DST}.copy2 AS ${DB_OVL}.${T} ENGINE = Memory" 2>&1 | grep -o ACCESS_DENIED | uniq

echo '... nor for CLONE AS'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "CREATE TABLE ${DB_DST}.copy3 CLONE AS ${DB_OVL}.${T}" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'No copies were created'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${DB_DST}' AND name LIKE 'copy%'"

echo 'SHOW COLUMNS on both the Overlay and the underlying database allows CREATE TABLE ... AS'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "CREATE TABLE ${DB_DST}.copy1 AS ${DB_OVL}.${T}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${DB_DST}' AND name = 'copy1'"

echo 'INSERT on the Overlay alone is not enough for CREATE MATERIALIZED VIEW ... TO through the facade'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "CREATE MATERIALIZED VIEW ${DB_DST}.mv1 TO ${DB_OVL}.${T} AS SELECT d, x, s FROM ${DB_DST}.feeder" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'The materialized view was not created'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${DB_DST}' AND name = 'mv1'"

echo 'INSERT on both the Overlay and the underlying database allows CREATE MATERIALIZED VIEW ... TO'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "CREATE MATERIALIZED VIEW ${DB_DST}.mv1 TO ${DB_OVL}.${T} AS SELECT d, x, s FROM ${DB_DST}.feeder"

echo 'Writes through the materialized view land in the source table'
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_DST}.feeder VALUES ('2000-01-02', 2, 'b')"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB_SRC}.${T}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_DST};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
