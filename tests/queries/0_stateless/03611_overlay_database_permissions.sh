#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique names per test run
SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_A="db_a_${SUF}"
DB_B="db_b_${SUF}"
DB_OVL="dboverlay_${SUF}"

T_A="t_a"
T_B="t_b"

USER_OK="u_ok_${SUF}"
USER_BAD="u_bad_${SUF}"
USER_OVL="u_ovl_${SUF}"
USER_SRC="u_src_${SUF}"

# Clean slate
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_OK};
    DROP USER IF EXISTS ${USER_BAD};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_SRC};
"

# Prepare underlying data
${CLICKHOUSE_CLIENT} -nm --query "
    CREATE DATABASE ${DB_A} ENGINE = Atomic;
    CREATE DATABASE ${DB_B} ENGINE = Atomic;

    CREATE TABLE ${DB_A}.${T_A} (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${DB_B}.${T_B} (id UInt32, s String) ENGINE = MergeTree ORDER BY id;

    INSERT INTO ${DB_A}.${T_A} VALUES (1,'a1'), (2,'a2');
    INSERT INTO ${DB_B}.${T_B} VALUES (10,'b10'), (20,'b20');

    -- Create overlay facade that unions the two DBs.
    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_A}', '${DB_B}');
"

# Access checks use the database name as written in the query: reading through the Overlay
# requires a grant on the Overlay database itself; grants on the underlying databases govern
# only direct access and are not consulted for queries that go through the facade.
${CLICKHOUSE_CLIENT} -nm --query "
    CREATE USER ${USER_OK}  NOT IDENTIFIED;
    CREATE USER ${USER_BAD} NOT IDENTIFIED;
    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_SRC} NOT IDENTIFIED;

    -- OK user: grants on both the Overlay and the underlying databases.
    GRANT SELECT ON ${DB_OVL}.* TO ${USER_OK};
    GRANT SELECT ON ${DB_A}.* TO ${USER_OK};
    GRANT SELECT ON ${DB_B}.* TO ${USER_OK};

    -- OVL user: grant on the Overlay database only.
    GRANT SELECT ON ${DB_OVL}.* TO ${USER_OVL};

    -- SRC user: grant on an underlying database only.
    GRANT SELECT ON ${DB_A}.* TO ${USER_SRC};

    -- BAD user: no grants on either the Overlay or the underlying databases.
"

echo 'Sanity: default user can see overlay tables'
${CLICKHOUSE_CLIENT} -nm --query "
    SHOW TABLES FROM ${DB_OVL};
"

echo 'Able to Select from overlay tables with OK user with (Access granted)'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OK}" --query "
    SELECT count() FROM ${DB_OVL}.${T_A};
" >/dev/null && echo "Access granted"

${CLICKHOUSE_CLIENT} -nm --user="${USER_OK}" --query "
    SELECT count() FROM ${DB_OVL}.${T_B};
" >/dev/null && echo "Access granted"

echo 'Unable to Select from overlay tables with BAD user (ACCESS_DENIED)'
${CLICKHOUSE_CLIENT} -nm --user="${USER_BAD}" --query "
    SELECT count() FROM ${DB_OVL}.${T_A};
" 2>&1 | grep -o ACCESS_DENIED | uniq

${CLICKHOUSE_CLIENT} -nm --user="${USER_BAD}" --query "
    SELECT count() FROM ${DB_OVL}.${T_B};
" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'A grant on the Overlay database alone allows reading through the facade'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "
    SELECT count() FROM ${DB_OVL}.${T_A};
" >/dev/null && echo "Access granted"

echo 'A grant on the Overlay database does not allow reading the underlying database directly'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "
    SELECT count() FROM ${DB_A}.${T_A};
" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'A grant on an underlying database alone does not allow reading through the facade'
${CLICKHOUSE_CLIENT} -nm --user="${USER_SRC}" --query "
    SELECT count() FROM ${DB_OVL}.${T_A};
" 2>&1 | grep -o ACCESS_DENIED | uniq

${CLICKHOUSE_CLIENT} -nm --user="${USER_SRC}" --query "
    SELECT count() FROM ${DB_A}.${T_A};
" >/dev/null && echo "Access granted"

# INSERT through the facade resolves to a table owned by an underlying database, and the
# INSERT privilege is checked against that owning database — a grant on the Overlay alone
# must not allow writing into a source the user cannot write to directly.
${CLICKHOUSE_CLIENT} -nm --query "
    GRANT INSERT ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT INSERT ON ${DB_A}.* TO ${USER_SRC};
"

echo 'A grant on the Overlay database alone does not allow inserting through the facade'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "
    INSERT INTO ${DB_OVL}.${T_A} VALUES (100, 'x100');
" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'A grant on the underlying database allows inserting through the facade'
${CLICKHOUSE_CLIENT} -nm --user="${USER_SRC}" --query "
    INSERT INTO ${DB_OVL}.${T_A} VALUES (200, 'x200');
" >/dev/null && echo "Access granted"

echo 'The inserted row landed in the underlying table'
${CLICKHOUSE_CLIENT} -nm --query "
    SELECT count() FROM ${DB_A}.${T_A} WHERE id = 200;
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_OK};
    DROP USER IF EXISTS ${USER_BAD};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_SRC};
"
