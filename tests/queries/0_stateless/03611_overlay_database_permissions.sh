#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique names per test run
SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_A="db_overlay_a_${SUF}"
DB_B="db_overlay_b_${SUF}"
DB_OVL="dboverlay_${SUF}"

T_A="t_a_${SUF}"
T_B="t_b_${SUF}"

USER_OK="u_ok_${SUF}"
USER_BAD="u_bad_${SUF}"

# Clean slate
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_OK};
    DROP USER IF EXISTS ${USER_BAD};
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

# Users
${CLICKHOUSE_CLIENT} -nm --query "
    CREATE USER ${USER_OK}  NOT IDENTIFIED;
    CREATE USER ${USER_BAD} NOT IDENTIFIED;

    -- OK user: ONLY underlying grants (no overlay grants)
    GRANT SELECT ON ${DB_A}.* TO ${USER_OK};
    GRANT SELECT ON ${DB_B}.* TO ${USER_OK};

    -- BAD user: ONLY overlay grants (no underlying)
    GRANT SELECT ON ${DB_OVL}.* TO ${USER_BAD};
"

# Sanity: default user can see overlay tables
${CLICKHOUSE_CLIENT} -nm --query "
    SHOW TABLES FROM ${DB_OVL} ORDER BY name;
" >/dev/null 2>&1 && echo "overlay: tables visible (sanity)."

# Able to Select from overlay tables with OK user
${CLICKHOUSE_CLIENT} -nm --user="${USER_OK}" --query "
    SELECT count() FROM ${DB_OVL}.${T_A};
" >/dev/null 2>&1 && echo "Ok."

${CLICKHOUSE_CLIENT} -nm --user="${USER_OK}" --query "
    SELECT count() FROM ${DB_OVL}.${T_B};
" >/dev/null 2>&1 && echo "Ok."

# Unable to Select from overlay tables with BAD user
${CLICKHOUSE_CLIENT} -nm --user="${USER_BAD}" --query "
    SELECT count() FROM ${DB_OVL}.${T_A};
" 2>&1 | grep -o ACCESS_DENIED | uniq

${CLICKHOUSE_CLIENT} -nm --user="${USER_BAD}" --query "
    SELECT count() FROM ${DB_OVL}.${T_B};
" 2>&1 | grep -o ACCESS_DENIED | uniq

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_OK};
    DROP USER IF EXISTS ${USER_BAD};
"
