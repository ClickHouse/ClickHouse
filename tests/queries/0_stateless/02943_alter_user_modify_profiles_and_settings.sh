#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

test_user="test_user_02932_${CLICKHOUSE_DATABASE}_$RANDOM"
test_role="test_role_02932_${CLICKHOUSE_DATABASE}_$RANDOM"
test_profile="test_profile_02932_${CLICKHOUSE_DATABASE}_$RANDOM"

profile_a="profile_a_02932_${CLICKHOUSE_DATABASE}_$RANDOM"
profile_b="profile_b_02932_${CLICKHOUSE_DATABASE}_$RANDOM"
profile_c="profile_c_02932_${CLICKHOUSE_DATABASE}_$RANDOM"

$CLICKHOUSE_CLIENT -q "DROP PROFILE IF EXISTS ${profile_a};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE IF EXISTS ${profile_b};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE IF EXISTS ${profile_c};"

$CLICKHOUSE_CLIENT -q "CREATE PROFILE ${profile_a};"
$CLICKHOUSE_CLIENT -q "CREATE PROFILE ${profile_b};"
$CLICKHOUSE_CLIENT -q "CREATE PROFILE ${profile_c};"

function show_create()
{
    local type="$1"
    local name="$2"

    $CLICKHOUSE_CLIENT -q "SHOW CREATE $type ${name};" | sed -e "s/${name}/test_${type}/g" -e "s/${profile_a}/profile_a/g" -e "s/${profile_b}/profile_b/g" -e "s/${profile_c}/profile_c/g"
}

function run_test()
{
    local type="$1"
    local name="$2"

    $CLICKHOUSE_CLIENT -q "DROP $type IF EXISTS ${name};"

    $CLICKHOUSE_CLIENT -q "CREATE $type ${name};"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTINGS custom_x = 123, custom_y=56.5, custom_w='www', ADD PROFILES ${profile_a}, ${profile_b}"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL SETTINGS, DROP ALL PROFILES"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTINGS custom_x = 123, custom_y=56.5, custom_w='www', PROFILES ${profile_a}, ${profile_b}"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL SETTINGS, ALL PROFILES"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTING custom_x = 123, ADD SETTING custom_y=56.5, ADD SETTING custom_w='www', ADD PROFILE ${profile_a}, ADD PROFILE ${profile_b}"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL PROFILES, DROP ALL SETTINGS"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTING custom_x = 123 ADD SETTING custom_y=56.5 ADD SETTING custom_w='www' ADD PROFILE ${profile_a} ADD PROFILE ${profile_b}"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL SETTINGS DROP ALL PROFILES"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD SETTING custom_x = 123, custom_t = 7, custom_s = 'str'"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD PROFILE ${profile_a}"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP SETTING custom_t, ADD PROFILE ${profile_b}, ${profile_a}, MODIFY SETTING custom_s READONLY, custom_x = 321"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL SETTINGS, ALL PROFILES"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "DROP $type ${name};"
}

run_test user ${test_user}
run_test role ${test_role}
run_test profile ${test_profile}

$CLICKHOUSE_CLIENT -q "DROP PROFILE ${profile_a};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE ${profile_b};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE ${profile_c};"
