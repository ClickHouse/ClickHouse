#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Column tokens in system.completions follow the same dual-grant contract as system.columns:
# for a table reached through a read-only Overlay facade, SHOW COLUMNS must be granted on both
# the facade name and the underlying source table. A user with SHOW TABLES on the source (so the
# table token is visible) and SHOW on the facade only must not see the source column names.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"
TBL="tbl_${SUF}"
COL="secret_col_${SUF}"
USER_OVL="u_ovl_${SUF}"   # SHOW on the facade, but only SHOW TABLES on the source
USER_DUAL="u_dual_${SUF}" # SHOW on the facade and SHOW COLUMNS on the source too

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.${TBL} (${COL} UInt32) ENGINE = MergeTree ORDER BY ${COL};

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT SHOW TABLES ON ${DB_SRC}.${TBL} TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SHOW ON ${DB_SRC}.${TBL} TO ${USER_DUAL};
"

echo 'Sanity: default user sees the table and the column tokens'
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.completions WHERE context = 'table' AND word = '${TBL}'"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.completions WHERE context = 'column' AND word = '${COL}'"

echo 'Facade SHOW + source SHOW TABLES only: the table token is visible, the column token is not'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() > 0 FROM system.completions WHERE context = 'table' AND word = '${TBL}'"
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() FROM system.completions WHERE context = 'column' AND word = '${COL}'"

echo 'Facade SHOW + source SHOW COLUMNS: the column token is visible'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" --query "SELECT count() > 0 FROM system.completions WHERE context = 'column' AND word = '${COL}'"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
