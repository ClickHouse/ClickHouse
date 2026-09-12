#!/usr/bin/env bash
# Tags: no-parallel

# Disabled parallel for two reasons. RESTORE can only restore either all access entities or none
# (it can't restore only the entities added by the current test run), so a RESTORE from a parallel
# test run could recreate our entities before we expect that. And this test asserts that a RESTORE
# refreshes the access caches by itself, which any concurrent access DDL would also do, hiding a
# regression.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
policy="policy_${CLICKHOUSE_TEST_UNIQUE_NAME}"
backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${user};
CREATE TABLE ${CLICKHOUSE_DATABASE}.t (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.t SELECT number FROM numbers(10);
CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
CREATE ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.t USING id < 3 TO ${user};
"

echo "-- enforced when created by DDL"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count(), max(id) FROM ${CLICKHOUSE_DATABASE}.t"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.users, TABLE system.row_policies TO ${backup_name} FORMAT Null"

${CLICKHOUSE_CLIENT} -m --query "
DROP ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.t;
DROP USER ${user};
"

${CLICKHOUSE_CLIENT} --query "RESTORE TABLE system.users, TABLE system.row_policies FROM ${backup_name} FORMAT Null"

echo "-- restored"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.row_policies WHERE short_name = '${policy}' AND database = '${CLICKHOUSE_DATABASE}' AND table = 't'"

echo "-- enforced right after RESTORE, without SYSTEM RELOAD USERS"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count(), max(id) FROM ${CLICKHOUSE_DATABASE}.t"

${CLICKHOUSE_CLIENT} -m --query "
DROP ROW POLICY IF EXISTS ${policy} ON ${CLICKHOUSE_DATABASE}.t;
DROP USER IF EXISTS ${user};
"
