#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS test_user";
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS test_database";

${CLICKHOUSE_CLIENT} --query "CREATE USER test_user;";
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE test_database COMMENT 'test database with comment';";

${CLICKHOUSE_CLIENT} --query "GRANT ALTER MODIFY DATABASE COMMENT ON test_database.* TO test_user;";

${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR test_user;";

${CLICKHOUSE_CLIENT} --user=test_user --query "SHOW GRANTS FOR test_user" | sed 's/ TO.*//';

${CLICKHOUSE_CLIENT} --user=test_user --query "SELECT name, comment FROM system.databases WHERE name = 'test_database';";

${CLICKHOUSE_CLIENT} --user=test_user --query "ALTER DATABASE test_database MODIFY COMMENT 'new comment on database';"

${CLICKHOUSE_CLIENT} --user=test_user --query "SELECT name, comment FROM system.databases WHERE name = 'test_database';";

${CLICKHOUSE_CLIENT} --query "REVOKE ALTER MODIFY DATABASE COMMENT ON test_database.* FROM test_user;";

${CLICKHOUSE_CLIENT} --user=test_user --query "SHOW GRANTS FOR test_user;";

${CLICKHOUSE_CLIENT} --user=test_user --query "ALTER DATABASE test_database MODIFY COMMENT 'test alter comment after revoking;'  -- { serverError ACCESS_DENIED } ";

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS test_user";

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS test_database";