#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Management operations reached through a read-only Overlay facade follow the same dual-grant
# contract as the regular read path: the facade name resolves to the underlying source table, so
# the operation requires the grant on *both* the facade and the source, and the facade must not
# widen access.
#
# Covered here:
#   - CHECK TABLE ov.t: a user with CHECK on the facade alone is rejected; CHECK on both passes.
#   - KILL MUTATION on a facade row of system.mutations: a user with ALTER on the facade alone
#     (plus SHOW on the source, so the row is visible) is rejected; ALTER on both kills it.
# KILL PART_MOVE TO SHARD shares the identical source-side guard, but driving it requires a
# multi-shard replicated setup with the experimental part-moves feature, so it is not tested here.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_SRC="db_src_${SUF}"
DB_OVL="dbovl_${SUF}"
T="t_ck"

USER_OVL="u_ck_ovl_${SUF}"   # grants on the facade only (plus SHOW on the source)
USER_DUAL="u_ck_dual_${SUF}" # grants on the facade and on the source

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

    -- KILL MUTATION reads system.mutations with the user's context, and system.mutations is not
    -- in the implicitly accessible system tables, so both users need an explicit grant on it —
    -- this way the facade-only user's rejection can only come from the source-side ALTER check.
    GRANT CHECK ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT ALTER DELETE ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT SHOW TABLES ON ${DB_SRC}.* TO ${USER_OVL};
    GRANT SELECT ON system.mutations TO ${USER_OVL};

    GRANT CHECK ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT CHECK ON ${DB_SRC}.* TO ${USER_DUAL};
    GRANT ALTER DELETE ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT ALTER DELETE ON ${DB_SRC}.* TO ${USER_DUAL};
    GRANT SELECT ON system.mutations TO ${USER_DUAL};
"

echo 'CHECK on the Overlay database alone is not enough to CHECK TABLE through the facade'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "CHECK TABLE ${DB_OVL}.${T} SETTINGS check_query_single_value_result = 1" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'CHECK on both the Overlay and the underlying database succeeds'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "CHECK TABLE ${DB_OVL}.${T} SETTINGS check_query_single_value_result = 1"

# An invalid mutation stays in `system.mutations` with `is_done = 0` until it is killed,
# so there is no race between creating it and killing it.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${DB_SRC}.${T} DELETE WHERE toUInt32(s) = 1"

echo 'ALTER on the Overlay database alone is not enough to KILL MUTATION through the facade row'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "KILL MUTATION WHERE database = '${DB_OVL}' AND table = '${T}'" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'The mutation is still there'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = '${DB_SRC}' AND table = '${T}' AND is_done = 0"

echo 'ALTER on both the Overlay and the underlying database kills the mutation through the facade row'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "KILL MUTATION WHERE database = '${DB_OVL}' AND table = '${T}'" | cut -f1

echo 'No mutations are left'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = '${DB_SRC}' AND table = '${T}' AND is_done = 0"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
