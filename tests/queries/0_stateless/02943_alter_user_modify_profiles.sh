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
profile_d="profile_d_02932_${CLICKHOUSE_DATABASE}_$RANDOM"
profile_e="profile_e_02932_${CLICKHOUSE_DATABASE}_$RANDOM"

$CLICKHOUSE_CLIENT -q "DROP PROFILE IF EXISTS ${profile_a};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE IF EXISTS ${profile_b};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE IF EXISTS ${profile_c};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE IF EXISTS ${profile_d};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE IF EXISTS ${profile_e};"

$CLICKHOUSE_CLIENT -q "CREATE PROFILE ${profile_a};"
$CLICKHOUSE_CLIENT -q "CREATE PROFILE ${profile_b};"
$CLICKHOUSE_CLIENT -q "CREATE PROFILE ${profile_c};"
$CLICKHOUSE_CLIENT -q "CREATE PROFILE ${profile_d};"
$CLICKHOUSE_CLIENT -q "CREATE PROFILE ${profile_e};"

function show_create()
{
    local type="$1"
    local name="$2"

    $CLICKHOUSE_CLIENT -q "SHOW CREATE $type ${name};" | sed -e "s/${name}/test_${type}/g" -e "s/${profile_a}/profile_a/g" -e "s/${profile_b}/profile_b/g" -e "s/${profile_c}/profile_c/g" -e "s/${profile_d}/profile_d/g" -e "s/${profile_e}/profile_e/g"
}

function run_test()
{
    local type="$1"
    local name="$2"

    $CLICKHOUSE_CLIENT -q "DROP $type IF EXISTS ${name};"

    $CLICKHOUSE_CLIENT -q "CREATE $type ${name};"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD PROFILE ${profile_a};"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD PROFILE ${profile_b};"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD PROFILES ${profile_a}, ${profile_c};"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} ADD PROFILE ${profile_d} DROP PROFILES ${profile_b}, ${profile_c};"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL PROFILES, ADD PROFILE ${profile_e};"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "ALTER $type ${name} DROP ALL PROFILES;"
    show_create $type ${name}

    $CLICKHOUSE_CLIENT -q "DROP $type ${name};"
}

run_test user ${test_user}
run_test role ${test_role}
run_test profile ${test_profile}

${CLICKHOUSE_CLIENT} --query="ALTER USER user1 DROP ALL PROFILES,,,,,,ADD PROFILE a" 2>&1 | grep -F -q 'Syntax error' && echo 'OK' || echo 'FAIL'
${CLICKHOUSE_CLIENT} --query="ALTER USER user1 ADD PROFILE a, DROP ALL PROFILES" 2>&1 | grep -F -q 'Syntax error' && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "DROP PROFILE ${profile_a};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE ${profile_b};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE ${profile_c};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE ${profile_d};"
$CLICKHOUSE_CLIENT -q "DROP PROFILE ${profile_e};"
