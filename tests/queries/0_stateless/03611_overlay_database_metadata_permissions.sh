#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Metadata access through a read-only Overlay facade follows the same dual-grant contract as reads
# and INSERTs: SHOW TABLES, SHOW CREATE TABLE, DESCRIBE, SHOW COLUMNS, EXISTS and the facade rows
# of system.tables / system.columns expose a table only when the corresponding SHOW privilege is
# granted on *both* the facade and the underlying source table. The facade must not widen
# visibility: a user with SHOW on the facade alone can neither enumerate source tables nor read
# their schema through it.

# Unique names per test run
SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_A="db_a_${SUF}"
DB_B="db_b_${SUF}"
DB_OVL="dboverlay_${SUF}"

T_A="t_a"
T_B="t_b"

USER_OVL="u_meta_ovl_${SUF}"   # SHOW on the facade only
USER_DUAL="u_meta_dual_${SUF}" # SHOW on the facade and on db_a (but not on db_b)

# Clean slate
${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"

${CLICKHOUSE_CLIENT} -nm --query "
    CREATE DATABASE ${DB_A} ENGINE = Atomic;
    CREATE DATABASE ${DB_B} ENGINE = Atomic;

    CREATE TABLE ${DB_A}.${T_A}
    (
        id UInt32,
        s String,
        INDEX idx s TYPE minmax GRANULARITY 1,
        CONSTRAINT cnstr CHECK id < 1000,
        PROJECTION proj (SELECT s, count() GROUP BY s)
    ) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${DB_B}.${T_B}
    (
        id UInt32,
        s String,
        INDEX idx s TYPE minmax GRANULARITY 1,
        CONSTRAINT cnstr CHECK id < 1000,
        PROJECTION proj (SELECT s, count() GROUP BY s)
    ) ENGINE = MergeTree ORDER BY id;

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_A}', '${DB_B}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW ON ${DB_A}.* TO ${USER_DUAL};

    -- Reading these system tables needs an explicit SELECT grant (unlike system.tables/columns).
    -- The row-level SHOW privilege filter is what must hide the source rows through the facade.
    GRANT SELECT ON system.data_skipping_indices TO ${USER_OVL}, ${USER_DUAL};
    GRANT SELECT ON system.projections TO ${USER_OVL}, ${USER_DUAL};
    GRANT SELECT ON system.constraints TO ${USER_OVL}, ${USER_DUAL};
"

echo 'Sanity: default user sees both source tables through the facade'
${CLICKHOUSE_CLIENT} -nm --query "SHOW TABLES FROM ${DB_OVL};"

echo 'Facade-only SHOW grant: SHOW TABLES through the facade lists nothing'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "SHOW TABLES FROM ${DB_OVL};" | wc -l

echo 'Facade-only SHOW grant: EXISTS reports an existing source table as nonexistent, same as a missing one'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "EXISTS TABLE ${DB_OVL}.${T_A};"
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "EXISTS TABLE ${DB_OVL}.no_such_table;"

echo 'Facade-only SHOW grant: SHOW CREATE TABLE is denied'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "SHOW CREATE TABLE ${DB_OVL}.${T_A};" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Facade-only SHOW grant: DESCRIBE is denied'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "DESCRIBE TABLE ${DB_OVL}.${T_A};" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Facade-only SHOW grant: SHOW COLUMNS lists nothing'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "SHOW COLUMNS FROM ${T_A} FROM ${DB_OVL};" | wc -l

echo 'Facade-only SHOW grant: system.tables and system.columns list nothing for the facade'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "
    SELECT count() FROM system.tables WHERE database = '${DB_OVL}'
    SETTINGS show_remote_databases_in_system_tables = 1;
"
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "
    SELECT count() FROM system.columns WHERE database = '${DB_OVL}'
    SETTINGS show_remote_databases_in_system_tables = 1;
"

echo 'Facade-only SHOW grant: system.data_skipping_indices, system.projections and system.constraints list nothing for the facade'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "
    SELECT count() FROM system.data_skipping_indices WHERE database = '${DB_OVL}'
    SETTINGS show_remote_databases_in_system_tables = 1;
"
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "
    SELECT count() FROM system.projections WHERE database = '${DB_OVL}'
    SETTINGS show_remote_databases_in_system_tables = 1;
"
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "
    SELECT count() FROM system.constraints WHERE database = '${DB_OVL}'
    SETTINGS show_remote_databases_in_system_tables = 1;
"

echo 'Facade-only SHOW grant: SHOW INDEXES exposes no secondary index'
${CLICKHOUSE_CLIENT} -nm --user="${USER_OVL}" --query "SHOW INDEXES FROM ${T_A} FROM ${DB_OVL};" | grep -c idx

echo 'Dual SHOW grants: SHOW TABLES lists only the tables of the granted source'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SHOW TABLES FROM ${DB_OVL};"

echo 'Dual SHOW grants: EXISTS sees the granted source table but not the non-granted one'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "EXISTS TABLE ${DB_OVL}.${T_A};"
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "EXISTS TABLE ${DB_OVL}.${T_B};"

echo 'Dual SHOW grants: SHOW CREATE TABLE works for the granted source table'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SHOW CREATE TABLE ${DB_OVL}.${T_A};" >/dev/null && echo "Access granted"

echo 'Dual SHOW grants: SHOW CREATE TABLE is denied for the non-granted source table'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SHOW CREATE TABLE ${DB_OVL}.${T_B};" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Dual SHOW grants: DESCRIBE works for the granted source table'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "DESCRIBE TABLE ${DB_OVL}.${T_A};" >/dev/null && echo "Access granted"

echo 'Dual SHOW grants: DESCRIBE is denied for the non-granted source table'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "DESCRIBE TABLE ${DB_OVL}.${T_B};" 2>&1 | grep -o ACCESS_DENIED | uniq

echo 'Dual SHOW grants: SHOW COLUMNS lists the columns of the granted table only'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SHOW COLUMNS FROM ${T_A} FROM ${DB_OVL};" | wc -l
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SHOW COLUMNS FROM ${T_B} FROM ${DB_OVL};" | wc -l

echo 'Dual SHOW grants: system.data_skipping_indices, system.projections and system.constraints show the granted source only'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "
    SELECT table, name FROM system.data_skipping_indices WHERE database = '${DB_OVL}' ORDER BY table, name
    SETTINGS show_remote_databases_in_system_tables = 1;
"
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "
    SELECT table, name FROM system.projections WHERE database = '${DB_OVL}' ORDER BY table, name
    SETTINGS show_remote_databases_in_system_tables = 1;
"
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "
    SELECT table, name FROM system.constraints WHERE database = '${DB_OVL}' ORDER BY table, name
    SETTINGS show_remote_databases_in_system_tables = 1;
"

echo 'Dual SHOW grants: SHOW INDEXES exposes the granted source index but not the non-granted one'
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SHOW INDEXES FROM ${T_A} FROM ${DB_OVL};" | grep -c idx
${CLICKHOUSE_CLIENT} -nm --user="${USER_DUAL}" --query "SHOW INDEXES FROM ${T_B} FROM ${DB_OVL};" | grep -c idx

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
