#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

databasename="test_database_${CLICKHOUSE_TEST_UNIQUE_NAME}"
username="test_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${username}";
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${databasename}";

${CLICKHOUSE_CLIENT} --query "CREATE USER ${username};";
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${databasename} COMMENT 'test database with comment';";

${CLICKHOUSE_CLIENT} --query "GRANT ALTER MODIFY DATABASE COMMENT ON ${databasename}.* TO ${username};";

${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR ${username};";

${CLICKHOUSE_CLIENT} --user="${username}" --query "SHOW GRANTS FOR ${username}" | sed 's/ TO.*//';

${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT name, comment FROM system.databases WHERE name = '${databasename}';";

${CLICKHOUSE_CLIENT} --user="${username}" --query "ALTER DATABASE ${databasename} MODIFY COMMENT 'new comment on database';"

${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT name, comment FROM system.databases WHERE name = '${databasename}';";

${CLICKHOUSE_CLIENT} --query "REVOKE ALTER MODIFY DATABASE COMMENT ON ${databasename}.* FROM ${username};";

${CLICKHOUSE_CLIENT} --user="${username}" --query "SHOW GRANTS FOR ${username};";

${CLICKHOUSE_CLIENT} --user="${username}" --query "ALTER DATABASE ${databasename} MODIFY COMMENT 'test alter comment after revoking;' \
    -- { serverError ACCESS_DENIED } ";

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${username}";

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${databasename}";