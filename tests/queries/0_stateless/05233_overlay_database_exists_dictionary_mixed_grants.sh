#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A caller granted SHOW TABLES on a read-only Overlay facade but only SHOW DICTIONARIES on the
# underlying source still sees the source dictionary through EXISTS DICTIONARY, so the plain
# EXISTS form must agree: the broader facade-side grant must not hide an object that the narrower
# grant shows. A source *table* stays hidden for the same user, because SHOW TABLES was never
# granted on the source.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_MIX="u_mix_${SUF}" # SHOW TABLES on the facade, only SHOW DICTIONARIES on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_MIX};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.dict_data (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
    CREATE DICTIONARY ${DB_SRC}.d (id UInt64, val String)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'dict_data' DB '${DB_SRC}'))
        LIFETIME(0)
        LAYOUT(FLAT());
    CREATE TABLE ${DB_SRC}.t (x UInt64) ENGINE = MergeTree ORDER BY x;

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_MIX} NOT IDENTIFIED;
    GRANT SHOW ON ${DB_OVL}.* TO ${USER_MIX};
    GRANT SHOW DICTIONARIES ON ${DB_SRC}.* TO ${USER_MIX};
"

echo 'Facade SHOW plus source SHOW DICTIONARIES: EXISTS DICTIONARY and plain EXISTS agree on the dictionary'
${CLICKHOUSE_CLIENT} --user="${USER_MIX}" --query "EXISTS DICTIONARY ${DB_OVL}.d"
${CLICKHOUSE_CLIENT} --user="${USER_MIX}" --query "EXISTS ${DB_OVL}.d"

echo 'The same user does not see a source table: SHOW TABLES was granted on the facade only'
${CLICKHOUSE_CLIENT} --user="${USER_MIX}" --query "EXISTS ${DB_OVL}.t"

echo 'A missing name still reports 0'
${CLICKHOUSE_CLIENT} --user="${USER_MIX}" --query "EXISTS ${DB_OVL}.no_such_object"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_MIX};
"
