#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SYSTEM RELOAD DICTIONARY` and `SYSTEM UNLOAD DICTIONARY` keep their own unresolved name instead
# of the resolved table id the other `SYSTEM` commands use, so in the unqualified form the general
# fence does not see the database at all: the handler qualifies the name with the current database
# and reaches the source dictionary through the read-only `Overlay` facade. Both forms must be
# rejected with the facade as the current database, and must keep working on the source database.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};

    CREATE DATABASE ${DB_SRC};
    CREATE TABLE ${DB_SRC}.dict_data (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
    CREATE DICTIONARY ${DB_SRC}.d (id UInt64, val String)
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'dict_data' DB '${DB_SRC}'))
        LIFETIME(0)
        LAYOUT(FLAT());
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
"

# The client prints the error text more than once, so match presence rather than counting lines.
rejected() { grep -q TABLE_IS_PERMANENTLY_READ_ONLY && echo 1 || echo 0; }

echo 'qualified with the facade name: rejected'
${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD DICTIONARY ${DB_OVL}.d" 2>&1 | rejected
${CLICKHOUSE_CLIENT} --query "SYSTEM UNLOAD DICTIONARY ${DB_OVL}.d" 2>&1 | rejected

# `USE` in the same session rather than another `--database`, which `CLICKHOUSE_CLIENT` already sets.
echo 'unqualified with the facade as the current database: rejected'
${CLICKHOUSE_CLIENT} -nm --query "USE ${DB_OVL}; SYSTEM RELOAD DICTIONARY d" 2>&1 | rejected
${CLICKHOUSE_CLIENT} -nm --query "USE ${DB_OVL}; SYSTEM UNLOAD DICTIONARY d" 2>&1 | rejected

echo 'a missing dictionary name behind the facade is rejected too, so the fence is not an existence oracle'
${CLICKHOUSE_CLIENT} -nm --query "USE ${DB_OVL}; SYSTEM RELOAD DICTIONARY no_such_dictionary" 2>&1 | rejected

echo 'the underlying database is not rejected, in both forms'
${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD DICTIONARY ${DB_SRC}.d" 2>&1 | rejected
${CLICKHOUSE_CLIENT} -nm --query "USE ${DB_SRC}; SYSTEM RELOAD DICTIONARY d" 2>&1 | rejected
${CLICKHOUSE_CLIENT} -nm --query "USE ${DB_SRC}; SYSTEM UNLOAD DICTIONARY d" 2>&1 | rejected

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_SRC};
"
