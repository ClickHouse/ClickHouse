#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ALTER MODIFY CONSTRAINT` has a dedicated privilege under the `ALTER CONSTRAINT` group.
# A user holding only `ALTER ADD CONSTRAINT` / `ALTER DROP CONSTRAINT` must not be able to
# modify a constraint, while `ALTER MODIFY CONSTRAINT` or the parent `ALTER CONSTRAINT` must.

table="t_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_no_modify="u_no_modify_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_modify="u_modify_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_group="u_group_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
for u in "${user_no_modify}" "${user_modify}" "${user_group}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${u}"
done

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (a UInt32, CONSTRAINT c CHECK a < 10) ENGINE = MergeTree ORDER BY a"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_no_modify}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_modify}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_group}"

# Add and drop, but explicitly not modify.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER ADD CONSTRAINT, ALTER DROP CONSTRAINT ON ${table} TO ${user_no_modify}"
# The dedicated privilege.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER MODIFY CONSTRAINT ON ${table} TO ${user_modify}"
# The parent group privilege.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER CONSTRAINT ON ${table} TO ${user_group}"

echo "no modify privilege:"
${CLICKHOUSE_CLIENT} --user "${user_no_modify}" --query "ALTER TABLE ${table} MODIFY CONSTRAINT c CHECK a < 100" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "modify constraint privilege:"
${CLICKHOUSE_CLIENT} --user "${user_modify}" --query "ALTER TABLE ${table} MODIFY CONSTRAINT c CHECK a < 100" && echo "OK"

echo "constraint group privilege:"
${CLICKHOUSE_CLIENT} --user "${user_group}" --query "ALTER TABLE ${table} MODIFY CONSTRAINT c CHECK a < 1000" && echo "OK"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
for u in "${user_no_modify}" "${user_modify}" "${user_group}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP USER ${u}"
done
