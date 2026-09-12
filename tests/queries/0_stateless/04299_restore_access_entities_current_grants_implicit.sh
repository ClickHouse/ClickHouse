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
user_b="user_b_${CLICKHOUSE_TEST_UNIQUE_NAME}"
restoring_user="restoring_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${user_a};
DROP USER IF EXISTS ${user_b};
DROP USER IF EXISTS ${restoring_user};
"

# Create users holding explicit grants that the restoring user will only have implicitly:
# CREATE VIEW (implied by CREATE TABLE) and SHOW TABLES (implied by SELECT).
${CLICKHOUSE_CLIENT} -m --query "
CREATE USER ${user_a};
GRANT CREATE VIEW ON *.* TO ${user_a};
CREATE USER ${user_b};
GRANT SHOW TABLES ON *.* TO ${user_b};
"

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.users TO ${backup_name} FORMAT Null"

# For the implicit-privilege check, the restoring user holds only CREATE TABLE and
# SELECT WITH GRANT OPTION, which implicitly include CREATE VIEW and SHOW TABLES
# respectively. It has no explicit CREATE VIEW or SHOW TABLES grant option.
# It is also granted CREATE USER, CREATE ROLE and ROLE ADMIN so it can restore
# every access entity captured by "BACKUP TABLE system.users" (the backup is of
# the whole instance, which may contain users that have roles assigned);
# otherwise "RESTORE ALL" would fail with ACCESS_DENIED before reaching user_a.
${CLICKHOUSE_CLIENT} -m --query "
CREATE USER ${restoring_user};
GRANT CREATE USER, CREATE ROLE ON *.* TO ${restoring_user};
GRANT ROLE ADMIN ON *.* TO ${restoring_user};
GRANT CREATE TABLE ON *.* TO ${restoring_user} WITH GRANT OPTION;
GRANT SELECT ON *.* TO ${restoring_user} WITH GRANT OPTION;
"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER ${user_a};
DROP USER ${user_b};
"

echo "--- Restore with restore_access_entities_with_current_grants=true ---"
${CLICKHOUSE_CLIENT} --user="${restoring_user}" --query "RESTORE ALL FROM ${backup_name} SETTINGS restore_access_entities_with_current_grants=true FORMAT Null"

replacements="s/${user_a}/user_a/g; s/${user_b}/user_b/g"

echo "-- user_a grants (CREATE VIEW must be preserved via implicit CREATE TABLE grant option) --"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR ${user_a}" | sed "${replacements}"

echo "-- user_b grants (SHOW TABLES must be preserved via implicit SELECT grant option) --"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR ${user_b}" | sed "${replacements}"

# Cleanup
${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${user_a};
DROP USER IF EXISTS ${user_b};
DROP USER IF EXISTS ${restoring_user};
"
