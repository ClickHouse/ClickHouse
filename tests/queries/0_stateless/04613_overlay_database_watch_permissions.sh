#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `WATCH` through a read-only Overlay facade follows the same dual-grant contract as the regular
# read path: the facade name resolves to the underlying source table, so reading requires the
# SELECT grant on *both* the facade and the source. A user with SELECT on the facade alone must
# not be able to watch a source table through the facade.
#
# The access check runs before the storage's watch capability is probed, so we can drive it with a
# plain MergeTree table: a facade-only user is rejected with ACCESS_DENIED, while a dual-grant user
# passes the access check and only then fails because MergeTree does not support WATCH ("not
# supported") — which proves the access check let it through.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_SRC="db_src_${SUF}"
DB_OVL="dbovl_${SUF}"
T="t_w"

USER_OVL="u_w_ovl_${SUF}"   # SELECT on the facade only
USER_DUAL="u_w_dual_${SUF}" # SELECT on the facade and on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.${T} (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_SRC}.${T} VALUES (1, 'a');

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SELECT ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SELECT ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SELECT ON ${DB_SRC}.* TO ${USER_DUAL};
"

echo 'A grant on the Overlay database alone is not enough to WATCH through the facade'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "WATCH ${DB_OVL}.${T}" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Grants on both the Overlay and the underlying database pass the access check (WATCH then fails only because MergeTree does not support it)'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "WATCH ${DB_OVL}.${T}" 2>&1 | grep -oE 'ACCESS_DENIED|not supported' | head -1

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
