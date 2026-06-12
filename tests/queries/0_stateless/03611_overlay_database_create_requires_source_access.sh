#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
#
# Overlay database creation is gated by read access on the underlying databases:
# a user may run CREATE DATABASE ... ENGINE = Overlay(a, b) only if they hold
# SELECT on every underlying database a, b. Missing access on any single
# source denies the create. The stateless test configuration enables
# `table_engines_require_grant`, so all users are also granted
# TABLE ENGINE ON Overlay — what is being tested is the SELECT requirement.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_A="db_a_${SUF}"
DB_B="db_b_${SUF}"
T_A="t_a"
T_B="t_b"

USER_BOTH="u_both_${SUF}"   # SELECT on both sources  -> create allowed
USER_PART="u_part_${SUF}"   # SELECT on A only         -> create denied
USER_NONE="u_none_${SUF}"   # no source access              -> create denied

OVL_BOTH="ovl_both_${SUF}"
OVL_PART="ovl_part_${SUF}"
OVL_NONE="ovl_none_${SUF}"

# Clean slate
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${OVL_BOTH};
    DROP DATABASE IF EXISTS ${OVL_PART};
    DROP DATABASE IF EXISTS ${OVL_NONE};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_BOTH};
    DROP USER IF EXISTS ${USER_PART};
    DROP USER IF EXISTS ${USER_NONE};
"

# Underlying source databases.
${CLICKHOUSE_CLIENT} -nm --query "
    CREATE DATABASE ${DB_A} ENGINE = Atomic;
    CREATE DATABASE ${DB_B} ENGINE = Atomic;

    CREATE TABLE ${DB_A}.${T_A} (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${DB_B}.${T_B} (id UInt32, s String) ENGINE = MergeTree ORDER BY id;

    INSERT INTO ${DB_A}.${T_A} VALUES (1,'a1'), (2,'a2');
    INSERT INTO ${DB_B}.${T_B} VALUES (10,'b10'), (20,'b20');
"

# Users may create databases, but differ in their access to the sources.
${CLICKHOUSE_CLIENT} -nm --query "
    CREATE USER ${USER_BOTH} NOT IDENTIFIED;
    CREATE USER ${USER_PART} NOT IDENTIFIED;
    CREATE USER ${USER_NONE} NOT IDENTIFIED;

    GRANT CREATE DATABASE ON *.* TO ${USER_BOTH};
    GRANT CREATE DATABASE ON *.* TO ${USER_PART};
    GRANT CREATE DATABASE ON *.* TO ${USER_NONE};

    GRANT TABLE ENGINE ON Overlay TO ${USER_BOTH};
    GRANT TABLE ENGINE ON Overlay TO ${USER_PART};
    GRANT TABLE ENGINE ON Overlay TO ${USER_NONE};

    -- BOTH: read access on both sources.
    GRANT SELECT ON ${DB_A}.* TO ${USER_BOTH};
    GRANT SELECT ON ${DB_B}.* TO ${USER_BOTH};

    -- PART: read access on A only.
    GRANT SELECT ON ${DB_A}.* TO ${USER_PART};

    -- NONE: no source access.
"

echo 'User with SELECT on both sources can create the overlay'
${CLICKHOUSE_CLIENT} -nm --user="${USER_BOTH}" --query "
    CREATE DATABASE ${OVL_BOTH} ENGINE = Overlay('${DB_A}', '${DB_B}');
" >/dev/null 2>&1 && echo "CREATE: allowed" || echo "CREATE: denied"

echo 'And, once granted SELECT on the overlay, can read through it'
${CLICKHOUSE_CLIENT} -nm --query "GRANT SELECT ON ${OVL_BOTH}.* TO ${USER_BOTH};"
${CLICKHOUSE_CLIENT} -nm --user="${USER_BOTH}" --query "
    SELECT count() FROM ${OVL_BOTH}.${T_A};
" >/dev/null 2>&1 && echo "READ: allowed" || echo "READ: denied"

echo 'User with access to only one source cannot create the overlay (denied)'
${CLICKHOUSE_CLIENT} -nm --user="${USER_PART}" --query "
    CREATE DATABASE ${OVL_PART} ENGINE = Overlay('${DB_A}', '${DB_B}');
" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'User with no source access cannot create the overlay (denied)'
${CLICKHOUSE_CLIENT} -nm --user="${USER_NONE}" --query "
    CREATE DATABASE ${OVL_NONE} ENGINE = Overlay('${DB_A}', '${DB_B}');
" 2>&1 | grep -o ACCESS_DENIED | uniq

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${OVL_BOTH};
    DROP DATABASE IF EXISTS ${OVL_PART};
    DROP DATABASE IF EXISTS ${OVL_NONE};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_BOTH};
    DROP USER IF EXISTS ${USER_PART};
    DROP USER IF EXISTS ${USER_NONE};
"