#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_a="user_a_${CLICKHOUSE_TEST_UNIQUE_NAME}"
role_b="role_b_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
CREATE ROLE ${role_b} SETTINGS custom_x=1;
CREATE USER ${user_a} DEFAULT ROLE ${role_b} SETTINGS custom_x=2;
"

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.users, TABLE system.roles TO ${backup_name} FORMAT Null"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} FORMAT Null"

do_check()
{
    local replacements
    replacements="s/${user_a}/user_a/g; s/${role_b}/role_b/g"
    local check_info
    check_info=$(${CLICKHOUSE_CLIENT} -mq "
        SHOW CREATE USER ${user_a};
        SHOW GRANTS FOR ${user_a};
        SHOW CREATE ROLE ${role_b};
        SHOW GRANTS FOR ${role_b};
    " | sed "${replacements}")
    local expected
    expected=$'CREATE USER user_a DEFAULT ROLE role_b SETTINGS custom_x = 2\nGRANT role_b TO user_a\nCREATE ROLE role_b SETTINGS custom_x = 1'
    if [[ "${check_info}" != "${expected}" ]]; then
       echo "Assertion failed:"
       echo "\"${check_info}\""
       echo "!="
       echo "\"${expected}\""
    fi
}

echo "Everything dropped"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user_a}"
${CLICKHOUSE_CLIENT} --query "DROP ROLE ${role_b}"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} FORMAT Null"
do_check

echo "User dropped"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user_a}"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} FORMAT Null"
do_check

echo "Role dropped"
${CLICKHOUSE_CLIENT} --query "DROP ROLE ${role_b}"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} FORMAT Null"
do_check

echo "Nothing dropped"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} FORMAT Null"
do_check

echo "Nothing dropped, mode=replace"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} SETTINGS create_access='replace' FORMAT Null"
do_check

echo "Nothing dropped, mode=create"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} SETTINGS create_access='create' FORMAT Null" 2>&1 | grep -om1 "ACCESS_ENTITY_ALREADY_EXISTS"
do_check
