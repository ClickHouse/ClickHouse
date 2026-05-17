#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# no-parallel: creates and drops a global named collection
# no-replicated-database: named collections are server-global, not database-scoped

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_04142"
collection_name="${CLICKHOUSE_DATABASE}_test_nc_04142"

$CLICKHOUSE_CLIENT -q "
DROP NAMED COLLECTION IF EXISTS $collection_name;
DROP USER IF EXISTS $user_name;

CREATE NAMED COLLECTION $collection_name AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1)
    if echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "OK"
    fi
}

# Without NAMED COLLECTION grant: must fail with ACCESS_DENIED before any network call.
check_access "SET allow_experimental_ai_functions = 1; SELECT aiGenerate('$collection_name', 'hi')"

$CLICKHOUSE_CLIENT -q "GRANT NAMED COLLECTION ON $collection_name TO $user_name"

# With the grant: access check passes. The call still fails (unreachable host),
# but the failure must not be ACCESS_DENIED.
check_access "SET allow_experimental_ai_functions = 1; SELECT aiGenerate('$collection_name', 'hi')"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_name;
DROP NAMED COLLECTION IF EXISTS $collection_name;
"
