#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unknown-table hints ("Maybe you meant ...?") through a read-only Overlay facade follow the same
# fail-closed dual-grant contract as EXISTS / SHOW TABLES: a name is suggested only when the user
# holds the SHOW grant on both the facade and the underlying source table. A user granted only on
# the facade must not be able to provoke a hint naming a hidden source table from a misspelled
# query - that would leak the source name the other metadata paths already mask.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"
TBL="hidden_tbl_${SUF}"
TBL_TYPO="hidden_tbk_${SUF}"
USER_OVL="u_ovl_${SUF}"   # SHOW on the facade only, nothing on the source
USER_DUAL="u_dual_${SUF}" # SHOW on the facade and on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.${TBL} (n UInt32) ENGINE = MergeTree ORDER BY n;

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW ON ${DB_SRC}.* TO ${USER_DUAL};
"

echo 'Facade-only user: the misspelled name fails without suggesting the hidden source table'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW CREATE TABLE ${DB_OVL}.${TBL_TYPO}" 2>&1 | grep -q "Maybe you meant" && echo 1 || echo 0
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_OVL}.${TBL_TYPO}" 2>&1 | grep -q "${TBL}" && echo 1 || echo 0

echo 'Dual-grant user: the misspelled name is corrected to the source table through the facade'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SHOW CREATE TABLE ${DB_OVL}.${TBL_TYPO}" 2>&1 | grep -q "Maybe you meant.*${TBL}" && echo 1 || echo 0

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
