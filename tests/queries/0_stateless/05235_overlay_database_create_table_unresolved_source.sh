#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read-only `Overlay` facade resolves its sources lazily, so a source that is not registered right
# now is omitted from the union. `CREATE TABLE` must not follow that shortened list: the contract is
# the first *writable source database*, so while a configured source that precedes the creation
# target is unresolved the query is rejected instead of silently creating the table in a later
# source (where it would be shadowed, or would duplicate a table of the same name in the source that
# is currently missing). A source that comes *after* the creation target cannot change the placement
# and is therefore not an obstacle.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_A="db_a_${SUF}"
DB_B="db_b_${SUF}"
DB_OVL="db_ovl_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};

    CREATE DATABASE ${DB_A};
    CREATE DATABASE ${DB_B};
    CREATE TABLE ${DB_A}.t_owned (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_A}', '${DB_B}');
"

rejected() { grep -q UNKNOWN_DATABASE && echo 1 || echo 0; }
exists() { ${CLICKHOUSE_CLIENT} --query "EXISTS TABLE $1"; }

echo 'with every source resolved, the table is created in the first writable source'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_OVL}.t_new (id UInt64) ENGINE = MergeTree ORDER BY id" 2>&1 | rejected
exists "${DB_A}.t_new"
exists "${DB_B}.t_new"

${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DB_A}"

echo 'while the first source is unresolved, CREATE TABLE through the facade is rejected'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_OVL}.t_second (id UInt64) ENGINE = MergeTree ORDER BY id" 2>&1 | rejected

echo 'including a name the unresolved source already owns, so no duplicate is created'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_OVL}.t_owned (id UInt64) ENGINE = MergeTree ORDER BY id" 2>&1 | rejected

echo 'nothing landed in the later source'
exists "${DB_B}.t_second"
exists "${DB_B}.t_owned"

echo 'the underlying writable database still takes the table directly'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_B}.t_direct (id UInt64) ENGINE = MergeTree ORDER BY id" 2>&1 | rejected

${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${DB_A}"

echo 'once the source is back, the table is created in it again'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_OVL}.t_second (id UInt64) ENGINE = MergeTree ORDER BY id" 2>&1 | rejected
exists "${DB_A}.t_second"
exists "${DB_B}.t_second"

${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DB_B}"

echo 'a source that follows the creation target cannot change the placement, so it is not an obstacle'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_OVL}.t_third (id UInt64) ENGINE = MergeTree ORDER BY id" 2>&1 | rejected
exists "${DB_A}.t_third"

${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${DB_B}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_A};
    DROP DATABASE ${DB_B};
"
