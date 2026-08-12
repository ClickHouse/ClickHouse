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

# Runs both AI functions as the test user in one client invocation and emits one line
# per query — "ACCESS_DENIED" if that line of output mentions ACCESS_DENIED, "OK" otherwise.
# `aiEmbed` does not go through `FunctionBaseAI` and has its own access check, so we verify both.
# The zero-row variants (FROM (SELECT ... WHERE 0)) lock in that the access check runs even
# when no row reaches the function — otherwise an empty input could be used to bypass the grant.
function check_access_both()
{
    $CLICKHOUSE_CLIENT --user "$user_name" --password "password" --multiquery --ignore-error -q "
        SET allow_experimental_ai_functions = 1;
        SELECT aiGenerate('$collection_name', 'hi') FORMAT Null;
        SELECT 'SEP';
        SELECT aiEmbed('$collection_name', 'hi') FORMAT Null;
        SELECT 'SEP';
        SELECT aiGenerate('$collection_name', x) FROM (SELECT '' AS x WHERE 0) FORMAT Null;
        SELECT 'SEP';
        SELECT aiEmbed('$collection_name', x) FROM (SELECT '' AS x WHERE 0) FORMAT Null;
    " 2>&1 | awk '
        /ACCESS_DENIED/ { denied = 1; next }
        /^SEP$/ { print (denied ? "ACCESS_DENIED" : "OK"); denied = 0; next }
        END { print (denied ? "ACCESS_DENIED" : "OK") }
    '
}

# Without NAMED COLLECTION grant: must fail with ACCESS_DENIED before any network call,
# even on the zero-row queries (the access check must precede the empty-input fast path).
check_access_both

$CLICKHOUSE_CLIENT -q "GRANT NAMED COLLECTION ON $collection_name TO $user_name"

# With the grant: access check passes. The 1-row calls still fail (unreachable host),
# but the failure must not be ACCESS_DENIED. The 0-row calls now succeed cleanly.
check_access_both

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_name;
DROP NAMED COLLECTION IF EXISTS $collection_name;
"
