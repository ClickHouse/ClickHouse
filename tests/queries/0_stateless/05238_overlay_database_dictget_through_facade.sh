#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary created in a source database of a read-only Overlay facade is listed under the facade
# name (SHOW DICTIONARIES FROM <facade>, system.dictionaries), so the dictionary consumers that take
# the name as a string must resolve that name too: dictGet and its family, the dictionary() table
# function and the Dictionary table engine. They follow the facade's dual-grant rule: dictGet is
# required on both the facade name and the source dictionary, checked fail-closed before the source
# dictionary is loaded, and a denial names only the facade. A name that an earlier source shadows
# with an object of its own is not a dictionary of the facade and stays unresolvable.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_A="db_a_${SUF}"
DB_B="db_b_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_FACADE_ONLY="u_facade_${SUF}"   # dictGet on the facade only
USER_SOURCE_ONLY="u_source_${SUF}"   # dictGet on the source only
USER_BOTH="u_both_${SUF}"            # dictGet on the facade and on the source

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
    INSERT INTO ${DB_B}.dict_data VALUES (1, 'one'), (2, 'two');
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

    -- The Dictionary table engine with the facade name, in a plain database.
    CREATE TABLE ${DB_A}.via_engine (id UInt64, val String) ENGINE = Dictionary('${DB_OVL}.d');

    CREATE USER ${USER_FACADE_ONLY} NOT IDENTIFIED;
    GRANT dictGet ON ${DB_OVL}.* TO ${USER_FACADE_ONLY};

    CREATE USER ${USER_SOURCE_ONLY} NOT IDENTIFIED;
    GRANT dictGet ON ${DB_B}.* TO ${USER_SOURCE_ONLY};

    CREATE USER ${USER_BOTH} NOT IDENTIFIED;
    GRANT dictGet ON ${DB_OVL}.* TO ${USER_BOTH};
    GRANT dictGet ON ${DB_B}.* TO ${USER_BOTH};

    -- Table functions need this grant; SELECT is for the Dictionary engine table.
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER_FACADE_ONLY}, ${USER_SOURCE_ONLY}, ${USER_BOTH};
    GRANT SELECT ON ${DB_A}.via_engine TO ${USER_FACADE_ONLY}, ${USER_SOURCE_ONLY}, ${USER_BOTH};
"

echo 'Default user: the facade name resolves to the source dictionary in every consumer'
${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${DB_OVL}.d', 'val', toUInt64(1)), dictHas('${DB_OVL}.d', toUInt64(3)), dictGetOrDefault('${DB_OVL}.d', 'val', toUInt64(3), 'none')"
${CLICKHOUSE_CLIENT} -nm --query "USE ${DB_OVL}; SELECT dictGet('d', 'val', toUInt64(2))"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dictionary('${DB_OVL}.d') ORDER BY id"
${CLICKHOUSE_CLIENT} -nm --query "USE ${DB_OVL}; SELECT * FROM dictionary('d') ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${DB_A}.via_engine ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${DB_OVL}.d ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT database, name FROM system.dictionaries WHERE database = '${DB_OVL}'" | sed "s/_${SUF}//g"

echo 'The shadowed name denotes the table of the first source, not the dictionary of the second'
${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${DB_OVL}.shadow', 'val', toUInt64(1))" 2>&1 | grep -o -m1 'not found' || echo 'unexpected'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dictionary('${DB_OVL}.shadow')" 2>&1 | grep -o -m1 'not found' || echo 'unexpected'
${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${DB_B}.shadow', 'val', toUInt64(1))"

echo 'Facade grant only: denied before the source dictionary is reached'
${CLICKHOUSE_CLIENT} --user="${USER_FACADE_ONLY}" --query "SELECT dictGet('${DB_OVL}.d', 'val', toUInt64(1))" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'unexpected'
${CLICKHOUSE_CLIENT} --user="${USER_FACADE_ONLY}" --query "SELECT * FROM dictionary('${DB_OVL}.d')" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'unexpected'
${CLICKHOUSE_CLIENT} --user="${USER_FACADE_ONLY}" --query "SELECT * FROM ${DB_A}.via_engine" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'unexpected'

echo 'Source grant only: denied on the facade name, and the denial does not name the source database'
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" --query "SELECT dictGet('${DB_OVL}.d', 'val', toUInt64(1))" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'unexpected'
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" --query "SELECT dictGet('${DB_OVL}.d', 'val', toUInt64(1))" 2>&1 | grep -c "${DB_B}"
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" -nm --query "USE ${DB_OVL}; SELECT dictGet('d', 'val', toUInt64(1))" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'unexpected'
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" --query "SELECT * FROM dictionary('${DB_OVL}.d')" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'unexpected'
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" --query "SELECT * FROM ${DB_A}.via_engine" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'unexpected'
${CLICKHOUSE_CLIENT} --user="${USER_SOURCE_ONLY}" --query "SELECT dictGet('${DB_B}.d', 'val', toUInt64(1))"

echo 'Both grants: every consumer works through the facade name'
${CLICKHOUSE_CLIENT} --user="${USER_BOTH}" --query "SELECT dictGet('${DB_OVL}.d', 'val', toUInt64(1))"
${CLICKHOUSE_CLIENT} --user="${USER_BOTH}" -nm --query "USE ${DB_OVL}; SELECT dictGet('d', 'val', toUInt64(2))"
${CLICKHOUSE_CLIENT} --user="${USER_BOTH}" --query "SELECT * FROM dictionary('${DB_OVL}.d') ORDER BY id"
${CLICKHOUSE_CLIENT} --user="${USER_BOTH}" --query "SELECT * FROM ${DB_A}.via_engine ORDER BY id"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_FACADE_ONLY}, ${USER_SOURCE_ONLY}, ${USER_BOTH};
"
