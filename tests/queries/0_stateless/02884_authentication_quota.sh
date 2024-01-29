#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUOTA="2884_quota_$$"
USER="2884_user_$$"
ROLE="2884_role_$$"

echo "> Drop the user, quota, and role if those were created."
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"

echo "> Create the user with quota with the maximum single authentication attempt."
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY 'pass'"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1 TO ${USER}"

echo "> Check if the quota has been created."
${CLICKHOUSE_CLIENT} -q "SELECT COUNT(*) FROM system.quotas WHERE name = '${QUOTA}'"

echo "> Login to the user account using the wrong password."
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null"  2>&1 | grep -m1 -o 'password is incorrect'
echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
echo "> Try to login to the user account again. It should be allowed to login. Current tries 1 <= 1 max tries, so quota not exceed."
${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null"
echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
echo "> Login to the user account using the wrong password 2 times to exceed the quota."
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'
echo "> Login to the user account using the correct password, but login is unsuccesful (quota exceed)."
${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'
echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"

echo "> Alter the quota with MAX FAILED SEQUENTIAL AUTHENTICATIONS = 3"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 3 TO ${USER}"
echo "> Login to the user account using the wrong password."
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null"  2>&1 | grep -m1 -o 'password is incorrect'
echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
echo "> Increaseing MAX FAILED SEQUENTIAL AUTHENTICATIONS should now allow the user to login."
${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null"
echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"

echo "> Create the role with quota with the maximum single authentication attempt."
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT ${ROLE} to ${USER}"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1 TO ${ROLE}"
echo "> Login to the user account using the wrong password."
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
echo "> Try to login to the user account again. It should be allowed to login. Current tries 1 <= 1 max tries, so quota not exceed."
${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null"
echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
${CLICKHOUSE_CLIENT} -q "SELECT failed_sequential_authentications, max_failed_sequential_authentications FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
echo "> Login to the user account using the wrong password 2 times to exceed the quota."
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'
echo "> Check the failed_sequential_authentications, max_failed_sequential_authentications fields."
${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'

echo "> Alter the quota connected with role by setting MAX FAILED SEQUENTIAL AUTHENTICATIONS = 3"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 3 TO ${USER}"
echo "> Login to the user account using the wrong password."
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
echo "> Login to the user account using the correct password."
${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null"
echo "> Alter the quota connected with role by setting MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX FAILED SEQUENTIAL AUTHENTICATIONS = 1 TO ${USER}"
echo "> Login 2 times to the user account using the wrong password to exceed the quota"
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'password is incorrect'
echo "> Login to the user account using wrong password and check that the error is 'QUOTA_EXCEEDED'."
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'
echo "> Login to the user account using correct password and that the error is 'QUOTA_EXCEEDED'."
${CLICKHOUSE_CLIENT} --user ${USER} --password "pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'
echo "> Login to the user account using correct password and that the error is 'QUOTA_EXCEEDED'."
${CLICKHOUSE_CLIENT} --user ${USER} --password "wrong_pass" --query "select 1 format Null" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
