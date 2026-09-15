#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# EXISTS VIEW and EXISTS DICTIONARY through a read-only Overlay facade follow the same dual-grant
# contract as EXISTS TABLE: the object is reported as existing only when the corresponding SHOW
# privilege is granted on *both* the facade name and the underlying source object. With a
# facade-only grant the answer is 0 — indistinguishable from a missing name — and the source-side
# grant is checked from metadata only, before any lookup that would load the source object.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_OVL="u_ovl_${SUF}"   # SHOW on the facade only
USER_DUAL="u_dual_${SUF}" # SHOW on the facade and on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.dict_data (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
    CREATE DICTIONARY ${DB_SRC}.d (id UInt64, val String)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'dict_data' DB '${DB_SRC}'))
        LIFETIME(0)
        LAYOUT(FLAT());
    CREATE VIEW ${DB_SRC}.v AS SELECT 1 AS x;

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW ON ${DB_SRC}.* TO ${USER_DUAL};
"

echo 'Sanity: default user sees the view and the dictionary through the facade'
${CLICKHOUSE_CLIENT} --query "EXISTS VIEW ${DB_OVL}.v"
${CLICKHOUSE_CLIENT} --query "EXISTS DICTIONARY ${DB_OVL}.d"

echo 'Facade-only SHOW grant: EXISTS VIEW reports an existing source view as nonexistent, same as a missing one'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS VIEW ${DB_OVL}.v"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS VIEW ${DB_OVL}.no_such_view"

echo 'Facade-only SHOW grant: EXISTS DICTIONARY reports an existing source dictionary as nonexistent, same as a missing one'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS DICTIONARY ${DB_OVL}.d"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "EXISTS DICTIONARY ${DB_OVL}.no_such_dictionary"

echo 'Dual SHOW grants: EXISTS VIEW and EXISTS DICTIONARY see the source objects'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "EXISTS VIEW ${DB_OVL}.v"
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "EXISTS DICTIONARY ${DB_OVL}.d"

echo 'Dual SHOW grants: a missing name still reports 0'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "EXISTS VIEW ${DB_OVL}.no_such_view"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
