#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Operational metadata through a read-only Overlay facade follows the same dual-grant contract as
# schema metadata: the iterator-based system tables (system.parts, system.parts_columns,
# system.mutations, ...) expose a facade row only when SHOW is granted on *both* the facade and
# the underlying source table. A user with SHOW on the facade alone must not be able to read
# source part names, mutation state or other operational metadata through it.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_SRC="db_src_${SUF}"
DB_OVL="dbovl_${SUF}"
T="t_ops"

USER_OVL="u_ops_ovl_${SUF}"   # SHOW on the facade only
USER_DUAL="u_ops_dual_${SUF}" # SHOW on the facade and on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"

${CLICKHOUSE_CLIENT} -nm --query "
    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.${T} (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_SRC}.${T} VALUES (1, 'a');
    ALTER TABLE ${DB_SRC}.${T} UPDATE s = s WHERE 1 SETTINGS mutations_sync = 2;

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW ON ${DB_SRC}.* TO ${USER_DUAL};

    -- Reading these system tables needs an explicit SELECT grant. The row-level SHOW privilege
    -- filter is what must hide the facade rows from the facade-only user.
    GRANT SELECT ON system.parts TO ${USER_OVL}, ${USER_DUAL};
    GRANT SELECT ON system.parts_columns TO ${USER_OVL}, ${USER_DUAL};
    GRANT SELECT ON system.mutations TO ${USER_OVL}, ${USER_DUAL};
"

echo 'Sanity: default user sees the facade rows in system.parts and system.mutations'
${CLICKHOUSE_CLIENT} -nm --query "SELECT count() > 0 FROM system.parts WHERE database = '${DB_OVL}';"
${CLICKHOUSE_CLIENT} -nm --query "SELECT count() > 0 FROM system.mutations WHERE database = '${DB_OVL}';"

echo 'Facade-only SHOW grant: system.parts exposes no facade row'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "SELECT count() FROM system.parts WHERE database = '${DB_OVL}';"

echo 'Facade-only SHOW grant: system.parts_columns exposes no facade row'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "SELECT count() FROM system.parts_columns WHERE database = '${DB_OVL}';"

echo 'Facade-only SHOW grant: system.mutations exposes no facade row'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "SELECT count() FROM system.mutations WHERE database = '${DB_OVL}';"

echo 'Dual SHOW grants: the facade rows are visible'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SELECT count() > 0 FROM system.parts WHERE database = '${DB_OVL}';"
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SELECT count() > 0 FROM system.parts_columns WHERE database = '${DB_OVL}';"
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SELECT count() > 0 FROM system.mutations WHERE database = '${DB_OVL}';"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
