#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUOTA="2884_quota_${CLICKHOUSE_DATABASE}"
USER="2884_user_${CLICKHOUSE_DATABASE}"
ROLE="2884_role_${CLICKHOUSE_DATABASE}"


function login_test()
{
    echo "> Try to login to the user account with correct password"
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null"

    echo "> Login to the user account using the wrong password."
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'

    echo "> Quota is exceeded 1 >= 1. Login with correct password should fail."
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'

    echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
    ${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"

    echo "> Alter the quota with MAX FAILED SEQUENTIAL AUTHENTICATIONS = 4"
    ${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 4 TO ${USER}"

    echo "> Try to login to the user account with correct password"
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null"

    echo "> Successfull login should reset failed authentications counter. Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
    ${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"

    echo "> Login to the user account using the wrong password before exeeding the quota."
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'

    echo "> Also try to login with correct password. Quota should stay exceeded."
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'

    echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
    ${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"

    echo "> Reset the quota by increasing MAX FAILED SEQUENTIAL AUTHENTICATIONS and succesfull login"
    echo "> and check failed_sequential_authentications, max_failed_sequential_authentications."
    ${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 7 TO ${USER}"
    ${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null"
    ${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
}

echo "> Drop the user, quota, and role if those were created."
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"

echo "> Create the user with quota with the maximum single authentication attempt."
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY 'pass'"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1 TO ${USER}"

echo "> Check if the quota has been created."
${CLICKHOUSE_CLIENT} -q "SELECT COUNT(*) FROM system.quotas WHERE name = '${QUOTA}'"

login_test

echo " ---------------------------------------------------------------------------"
echo "> Create the role with quota with the maximum single authentication attempt."
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT ${ROLE} to ${USER}"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1 TO ${ROLE}"

login_test

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
