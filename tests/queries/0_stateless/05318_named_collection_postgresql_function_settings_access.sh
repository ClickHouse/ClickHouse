#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the PostgreSQL engine omitted from the fast build.

# The `SETTINGS` clause of the `postgresql` table function replaces the settings stored in a named collection,
# so it needs `SHOW NAMED COLLECTIONS SECRETS` and respects `NOT OVERRIDABLE`. The check runs before the connection,
# so the unreachable server only produces a connection error when the check passes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="${nc}_user"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS $nc;
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE USER $user;
    GRANT POSTGRES, CREATE TEMPORARY TABLE ON *.* TO $user;
    GRANT NAMED COLLECTION ON $nc TO $user;
    CREATE NAMED COLLECTION $nc AS
        host = '127.0.0.1', port = 1, user = 'user', password = 'secret', database = 'database', table = 'table',
        postgresql_connection_pool_size = 2;
"

# Classifies the error of the query: the secrets privilege is missing, the key is locked,
# or the check passed and the query only reached the unreachable server.
function error_code()
{
    local error
    if error=$(${CLICKHOUSE_CLIENT} --user "$user" --query "$1" 2>&1); then
        echo "Expected an error: $1"
        exit 1
    fi
    if [[ "$error" == *"SHOW NAMED COLLECTIONS SECRETS ON"* ]]; then
        echo 'secrets privilege required'
    elif [[ "$error" == *"Override not allowed for 'postgresql_connection_pool_size'"* ]]; then
        echo 'override not allowed'
    elif [[ "$error" == *"POSTGRESQL_CONNECTION_FAILURE"* ]]; then
        echo 'check passed'
    else
        echo "$error"
    fi
}

echo 'Replacing a stored setting without the secrets privilege'
error_code "SELECT * FROM postgresql($nc, SETTINGS postgresql_connection_pool_size = 1)"

echo 'Adding a setting that the collection does not store'
error_code "SELECT * FROM postgresql($nc, SETTINGS postgresql_connection_pool_wait_timeout = 1000)"

echo 'Replacing a stored setting with the secrets privilege'
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON $nc TO $user"
error_code "SELECT * FROM postgresql($nc, SETTINGS postgresql_connection_pool_size = 1)"

echo 'Replacing a stored setting that is not overridable'
${CLICKHOUSE_CLIENT} --query "ALTER NAMED COLLECTION $nc SET postgresql_connection_pool_size = 2 NOT OVERRIDABLE"
error_code "SELECT * FROM postgresql($nc, SETTINGS postgresql_connection_pool_size = 1)"
