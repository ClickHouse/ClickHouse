#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `MySQL` table engine is not built in the fast test.
#
# A new `MySQL` table adds a session value of `mysql_datatypes_support_level` that differs from the default to its
# `SETTINGS`. A `RESTORE` in such a session keeps the definition from the backup, which it requires to be unchanged.
# The engine does not connect at `CREATE` or `RESTORE` when the columns are given, so the host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ENGINE="MySQL('127.0.0.1:3306', 'db', 'tbl', 'user', 'password')"
# A `Replicated` database prints the status of each DDL query otherwise.
DDL_CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none"

function settings_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

echo "--- a new table stores the session value"
${DDL_CLIENT} --mysql_datatypes_support_level decimal --query "CREATE TABLE t_session (a Decimal(10, 2)) ENGINE = ${ENGINE}"
settings_of t_session

echo "--- a RESTORE in such a session keeps the definition from the backup"
${DDL_CLIENT} --query "CREATE TABLE t_backup (a Decimal(10, 2)) ENGINE = ${ENGINE}"
${DDL_CLIENT} --query "
    BACKUP TABLE t_backup TO Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;
    SET mysql_datatypes_support_level = 'decimal';
    RESTORE TABLE t_backup AS t_restored FROM Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_restored'"
settings_of t_restored
