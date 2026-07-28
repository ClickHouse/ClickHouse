#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SYSTEM` commands naming a read-only `Overlay` facade are rejected: the facade resolves to the
# tables of its source databases, so the command would act on data the caller only has a
# facade-scoped grant for. `SYSTEM FLUSH ASYNC INSERT QUEUE` names its targets in a separate list
# (`query.tables`), not in the single `query.table` / `query.database` pair the general check looks
# at, so it needs the same rejection applied to every entry of that list -- including an unqualified
# name, which resolves against the current database and can therefore be the facade too.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};

    CREATE DATABASE ${DB_SRC};
    CREATE TABLE ${DB_SRC}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
"

# The client prints the error text more than once, so match presence rather than counting lines.
rejected() { grep -q TABLE_IS_PERMANENTLY_READ_ONLY && echo 1 || echo 0; }

echo 'qualified with the facade name: rejected'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE ${DB_OVL}.t" 2>&1 | rejected

echo 'a missing table name behind the facade is rejected too, so the fence is not an existence oracle'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE ${DB_OVL}.no_such_table" 2>&1 | rejected

# `USE` in the same session rather than another `--database`, which `CLICKHOUSE_CLIENT` already sets.
echo 'unqualified with the facade as the current database: rejected'
${CLICKHOUSE_CLIENT} -nm --query "USE ${DB_OVL}; SYSTEM FLUSH ASYNC INSERT QUEUE t" 2>&1 | rejected

echo 'one facade entry in a list rejects the whole command'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE ${DB_SRC}.t, ${DB_OVL}.t" 2>&1 | rejected

echo 'the underlying database is not rejected'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE ${DB_SRC}.t" 2>&1 | rejected

echo 'the whole-server form is not rejected'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>&1 | rejected

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_SRC};
"
