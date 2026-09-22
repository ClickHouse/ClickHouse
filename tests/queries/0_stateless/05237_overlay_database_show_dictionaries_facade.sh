#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary created in a source database of a read-only Overlay facade is reachable through the
# facade name by the point lookups (SHOW CREATE DICTIONARY, EXISTS DICTIONARY), so the listing
# surface must agree: SHOW DICTIONARIES FROM <facade> and system.dictionaries rows with the facade
# name must show it, under the same dual-grant rule (SHOW DICTIONARIES on both the facade and the
# source dictionary), and must not show a name that an earlier source shadows, because through the
# facade that name denotes the earlier source's object, not this dictionary.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_A="db_a_${SUF}"
DB_B="db_b_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_FACADE_ONLY="u_facade_${SUF}"   # SHOW DICTIONARIES on the facade only
USER_SOURCE_ONLY="u_source_${SUF}"   # SHOW DICTIONARIES on the source only
USER_BOTH="u_both_${SUF}"            # SHOW DICTIONARIES on the facade and on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_FACADE_ONLY}, ${USER_SOURCE_ONLY}, ${USER_BOTH};

    CREATE DATABASE ${DB_A} ENGINE = Atomic;
    CREATE DATABASE ${DB_B} ENGINE = Atomic;

    -- ${DB_A} owns a *table* named shadow: through the facade, shadow denotes this table.
    CREATE TABLE ${DB_A}.shadow (x UInt64) ENGINE = MergeTree ORDER BY x;

    CREATE TABLE ${DB_B}.dict_data (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
    CREATE DICTIONARY ${DB_B}.d (id UInt64, val String)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'dict_data' DB '${DB_B}'))
        LIFETIME(0)
        LAYOUT(FLAT());
    -- Same name as the table in ${DB_A}: shadowed through the facade.
    CREATE DICTIONARY ${DB_B}.shadow (id UInt64, val String)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'dict_data' DB '${DB_B}'))
        LIFETIME(0)
        LAYOUT(FLAT());

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_A}', '${DB_B}');

    CREATE USER ${USER_FACADE_ONLY} NOT IDENTIFIED;
    GRANT SHOW DICTIONARIES ON ${DB_OVL}.* TO ${USER_FACADE_ONLY};

    CREATE USER ${USER_SOURCE_ONLY} NOT IDENTIFIED;
    GRANT SHOW DICTIONARIES ON ${DB_B}.* TO ${USER_SOURCE_ONLY};

    CREATE USER ${USER_BOTH} NOT IDENTIFIED;
    GRANT SHOW DICTIONARIES ON ${DB_OVL}.* TO ${USER_BOTH};
    GRANT SHOW DICTIONARIES ON ${DB_B}.* TO ${USER_BOTH};

    -- Reading system.dictionaries itself requires a grant when select_from_system_db_requires_grant is on.
    GRANT SELECT ON system.dictionaries TO ${USER_FACADE_ONLY}, ${USER_SOURCE_ONLY}, ${USER_BOTH};
"

echo 'Default user: the facade lists d, but not the shadowed name; the source lists both'
${CLICKHOUSE_CLIENT} --query "SHOW DICTIONARIES FROM ${DB_OVL}"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(database, '_${SUF}', ''), name, uuid = (SELECT uuid FROM system.dictionaries WHERE database = '${DB_B}' AND name = 'd')
    FROM system.dictionaries WHERE database IN ('${DB_OVL}', '${DB_A}', '${DB_B}') ORDER BY database, name"

echo 'The facade row agrees with the point lookups on the facade name'
${CLICKHOUSE_CLIENT} --query "EXISTS DICTIONARY ${DB_OVL}.d"
${CLICKHOUSE_CLIENT} --query "EXISTS DICTIONARY ${DB_OVL}.shadow"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DICTIONARY ${DB_OVL}.shadow" 2>&1 | grep -o -m1 'is not a DICTIONARY' || echo 'unexpected'

echo 'Facade grant only: nothing is listed through the facade, and EXISTS DICTIONARY agrees'
${CLICKHOUSE_CLIENT} --user="${USER_FACADE_ONLY}" --query "SHOW DICTIONARIES FROM ${DB_OVL}"
${CLICKHOUSE_CLIENT} --user="${USER_FACADE_ONLY}" --query "SELECT count() FROM system.dictionaries WHERE database = '${DB_OVL}'"
${CLICKHOUSE_CLIENT} --user="${USER_FACADE_ONLY}" --query "EXISTS DICTIONARY ${DB_OVL}.d"

echo 'Source grant only: the source is listed, the facade is not'
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" --query "SHOW DICTIONARIES FROM ${DB_B}"
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" --query "SHOW DICTIONARIES FROM ${DB_OVL}"
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" --query "SELECT count() FROM system.dictionaries WHERE database = '${DB_OVL}'"

echo 'Both grants: the facade lists d and EXISTS DICTIONARY agrees'
${CLICKHOUSE_CLIENT} --user="${USER_BOTH}" --query "SHOW DICTIONARIES FROM ${DB_OVL}"
${CLICKHOUSE_CLIENT} --user="${USER_BOTH}" --query "EXISTS DICTIONARY ${DB_OVL}.d"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_FACADE_ONLY}, ${USER_SOURCE_ONLY}, ${USER_BOTH};
"
