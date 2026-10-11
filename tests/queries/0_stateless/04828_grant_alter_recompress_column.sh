#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ALTER TABLE ... RECOMPRESS COLUMN` requires the dedicated `ALTER RECOMPRESS COLUMN` privilege
# (a column-level privilege under the `ALTER COLUMN` group). A user without it -- even one holding
# other `ALTER COLUMN` privileges, or holding it for a different column only -- must be rejected,
# while the dedicated privilege or the parent `ALTER COLUMN` group must be enough.

table="t_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_none="u_none_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_other_column="u_other_col_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_recompress="u_recompress_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_group="u_group_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
for u in "${user_none}" "${user_other_column}" "${user_recompress}" "${user_group}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${u}"
done

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (id UInt64, s String CODEC(ZSTD)) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${table} SELECT number, toString(number) FROM numbers(1000)"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_none}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_other_column}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_recompress}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_group}"

# Other privileges of the `ALTER COLUMN` group, but explicitly not `ALTER RECOMPRESS COLUMN`.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER MODIFY COLUMN, ALTER RENAME COLUMN ON ${table} TO ${user_none}"
# The dedicated privilege, but for a different column.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER RECOMPRESS COLUMN(id) ON ${table} TO ${user_other_column}"
# The dedicated privilege for the target column.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER RECOMPRESS COLUMN(s) ON ${table} TO ${user_recompress}"
# The parent group privilege.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER COLUMN ON ${table} TO ${user_group}"

echo "no recompress privilege:"
${CLICKHOUSE_CLIENT} --user "${user_none}" --query "ALTER TABLE ${table} RECOMPRESS COLUMN s" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "privilege on another column:"
${CLICKHOUSE_CLIENT} --user "${user_other_column}" --query "ALTER TABLE ${table} RECOMPRESS COLUMN s" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "recompress column privilege:"
${CLICKHOUSE_CLIENT} --user "${user_recompress}" --mutations_sync 2 --query "ALTER TABLE ${table} RECOMPRESS COLUMN s" && echo "OK"

echo "column group privilege:"
${CLICKHOUSE_CLIENT} --user "${user_group}" --mutations_sync 2 --query "ALTER TABLE ${table} RECOMPRESS COLUMN s" && echo "OK"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
for u in "${user_none}" "${user_other_column}" "${user_recompress}" "${user_group}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP USER ${u}"
done
