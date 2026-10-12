#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

test_user="test_user_04365_${CLICKHOUSE_DATABASE}_$RANDOM"
test_role="test_role_04365_${CLICKHOUSE_DATABASE}_$RANDOM"
test_profile="test_profile_04365_${CLICKHOUSE_DATABASE}_$RANDOM"

function show_create()
{
    local type="$1"
    local name="$2"
    $CLICKHOUSE_CLIENT -q "SHOW CREATE $type ${name};" | sed "s/${name}/test_${type}/g"
}

# `SET` is an alias for `MODIFY SETTING`: it modifies individual settings in place,
# unlike `SETTINGS` which replaces the whole settings list.
function run_test()
{
    local type="$1"
    local name="$2"

    $CLICKHOUSE_CLIENT -q "DROP $type IF EXISTS ${name};"
    $CLICKHOUSE_CLIENT -q "CREATE $type ${name} SETTINGS max_threads = 4, max_memory_usage = 1000000;"
    show_create $type ${name}

    # SET one setting in place: max_memory_usage stays.
    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} SET max_threads = 8;"
    show_create $type ${name}

    # SET several settings in place.
    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} SET max_threads = 2, max_execution_time = 100;"
    show_create $type ${name}

    # SET with MIN / MAX / constraint, same as MODIFY SETTING.
    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} SET max_threads = 4 MIN 1 MAX 8 CONST;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "DROP $type ${name};"
}

run_test user ${test_user}
run_test role ${test_role}
run_test profile ${test_profile}

echo '--- SET is equivalent to MODIFY SETTING ---'
u1="test_user_04365_eq1_${CLICKHOUSE_DATABASE}_$RANDOM"
u2="test_user_04365_eq2_${CLICKHOUSE_DATABASE}_$RANDOM"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${u1}, ${u2};"
$CLICKHOUSE_CLIENT -q "CREATE USER ${u1} SETTINGS max_threads = 4, max_memory_usage = 1000000;"
$CLICKHOUSE_CLIENT -q "CREATE USER ${u2} SETTINGS max_threads = 4, max_memory_usage = 1000000;"
$CLICKHOUSE_CLIENT -q "ALTER USER ${u1} SET max_threads = 8;"
$CLICKHOUSE_CLIENT -q "ALTER USER ${u2} MODIFY SETTING max_threads = 8;"
diff <(show_create user ${u1}) <(show_create user ${u2}) && echo 'SET == MODIFY SETTING'
$CLICKHOUSE_CLIENT -q "DROP USER ${u1}, ${u2};"

echo '--- SET round-trips through the parser as MODIFY SETTING ---'
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('ALTER USER u SET max_threads = 4');"
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('ALTER USER u SET max_threads = 4, max_memory_usage = 1000000');"
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('ALTER ROLE r SET max_threads = 4 MIN 1 MAX 8 READONLY');"
