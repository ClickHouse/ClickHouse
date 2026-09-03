#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

test_user="test_user_02932_${CLICKHOUSE_DATABASE}_$RANDOM"
test_role="test_role_02932_${CLICKHOUSE_DATABASE}_$RANDOM"
test_profile="test_profile_02932_${CLICKHOUSE_DATABASE}_$RANDOM"

function show_create()
{
    local type="$1"
    local name="$2"

    $CLICKHOUSE_CLIENT -q "SHOW CREATE $type ${name};" | sed "s/${name}/test_${type}/g"
}

function run_test()
{
    local type="$1"
    local name="$2"

    $CLICKHOUSE_CLIENT -q "DROP $type IF EXISTS ${name};"

    $CLICKHOUSE_CLIENT -q "CREATE $type ${name};"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTING custom_a=100;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTING custom_b=200;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTINGS custom_b=300, custom_c=400;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTING custom_d=500 DROP SETTING custom_a;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTING custom_e=600 MIN 0 MAX 1000;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} MODIFY SETTING custom_e MAX 2000;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} MODIFY SETTING custom_e MIN 500;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} MODIFY SETTING custom_e=700;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTING custom_e=800 MIN 150;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP SETTINGS custom_b, custom_e;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} MODIFY SETTINGS custom_x=1, custom_y=2;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL SETTINGS, ADD SETTING custom_z=3;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL SETTINGS;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "DROP $type ${name};"
}

run_test user ${test_user}
run_test role ${test_role}
run_test profile ${test_profile}
