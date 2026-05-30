#!/usr/bin/env bash
# Tags: no-parallel

# Disabled parallel since RESTORE can only restore either all users or no users
# (it can't restore only users added by the current test run),
# so a RESTORE from a parallel test run could recreate our users before we expect that.

# Test that RESTORE ... SETTINGS restore_access_entities_with_current_grants=true
# accounts for implicit privileges, same as GRANT CURRENT GRANTS.
# For example, CREATE TABLE implicitly includes CREATE VIEW, so a restoring user
# holding only CREATE TABLE WITH GRANT OPTION must still be able to restore a
# CREATE VIEW grant instead of having it silently dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_a="user_a_${CLICKHOUSE_TEST_UNIQUE_NAME}"
restoring_user="restoring_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${user_a};
DROP USER IF EXISTS ${restoring_user};
"

# Create a user that has an explicit CREATE VIEW grant.
${CLICKHOUSE_CLIENT} -m --query "
CREATE USER ${user_a};
GRANT CREATE VIEW ON *.* TO ${user_a};
"

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.users TO ${backup_name} FORMAT Null"

# The restoring user holds only CREATE TABLE WITH GRANT OPTION, which implicitly
# includes CREATE VIEW. It has no explicit CREATE VIEW grant option.
${CLICKHOUSE_CLIENT} -m --query "
CREATE USER ${restoring_user};
GRANT CREATE USER ON *.* TO ${restoring_user};
GRANT CREATE TABLE ON *.* TO ${restoring_user} WITH GRANT OPTION;
"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user_a};"

echo "--- Restore with restore_access_entities_with_current_grants=true ---"
${CLICKHOUSE_CLIENT} --user="${restoring_user}" --query "RESTORE ALL FROM ${backup_name} SETTINGS restore_access_entities_with_current_grants=true FORMAT Null"

replacements="s/${user_a}/user_a/g"

echo "-- user_a grants (CREATE VIEW must be preserved via implicit CREATE TABLE grant option) --"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR ${user_a}" | sed "${replacements}"

# Cleanup
${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${user_a};
DROP USER IF EXISTS ${restoring_user};
"
