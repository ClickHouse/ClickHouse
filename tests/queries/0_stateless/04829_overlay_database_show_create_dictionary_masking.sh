#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SHOW CREATE DICTIONARY through a read-only Overlay facade must stay fail-closed for a
# dictionary-only user (SHOW DICTIONARIES on the facade, no source-side grant): a hidden source
# object - a dictionary, or a regular table - and a name that does not exist must all produce the
# same "There is no dictionary" error. A source-side ACCESS_DENIED surfacing from the facade's
# grant check would confirm that a source object with that name exists, turning the facade into an
# existence oracle. Only a user granted SHOW DICTIONARIES on both the facade and the source sees
# the definition.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_OVL="u_ovl_${SUF}"   # SHOW DICTIONARIES on the facade only
USER_DUAL="u_dual_${SUF}" # SHOW DICTIONARIES on the facade and on the source

expect_missing_dictionary()
{
    local user="$1"
    local name="$2"
    local output
    output=$(${CLICKHOUSE_CLIENT} --user="${user}" --query "SHOW CREATE DICTIONARY ${DB_OVL}.${name}" 2>&1)
    if [[ "${output}" == *"There is no dictionary ${DB_OVL}.${name}"* ]]; then
        echo 'OK: masked as a missing dictionary'
    else
        echo "FAIL: expected a missing-dictionary error, got: ${output}"
    fi
}

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

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW DICTIONARIES ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW DICTIONARIES ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW DICTIONARIES ON ${DB_SRC}.* TO ${USER_DUAL};
"

echo 'Facade-only grant: a hidden source dictionary is reported as a missing dictionary, not as denied'
expect_missing_dictionary "${USER_OVL}" "d"

echo 'Facade-only grant: a hidden source table is reported the same way'
expect_missing_dictionary "${USER_OVL}" "dict_data"

echo 'Facade-only grant: a missing name is reported the same way'
expect_missing_dictionary "${USER_OVL}" "no_such_name"

echo 'Dual grants: the dictionary definition is shown through the facade'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SHOW CREATE DICTIONARY ${DB_OVL}.d" | grep -c 'CREATE DICTIONARY'

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
