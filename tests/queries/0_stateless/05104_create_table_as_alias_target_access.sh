#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

target="target_${CLICKHOUSE_TEST_UNIQUE_NAME}"
alias_table="alias_${CLICKHOUSE_TEST_UNIQUE_NAME}"
copy="copy_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_no_grant="user_no_grant_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_column_grant="user_column_grant_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_table_grant="user_table_grant_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user_no_grant}, ${user_column_grant}, ${user_table_grant};

    CREATE TABLE ${target} (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${alias_table} ENGINE = Alias(currentDatabase(), ${target});

    CREATE USER ${user_no_grant}, ${user_column_grant}, ${user_table_grant} NOT IDENTIFIED;
    GRANT SHOW COLUMNS ON ${alias_table} TO ${user_no_grant}, ${user_column_grant}, ${user_table_grant};
    GRANT CREATE TABLE, DROP TABLE, SHOW COLUMNS ON ${copy} TO ${user_no_grant}, ${user_column_grant}, ${user_table_grant};
    GRANT TABLE ENGINE ON Memory TO ${user_no_grant}, ${user_column_grant}, ${user_table_grant};

    GRANT SELECT(value) ON ${target} TO ${user_column_grant};
    GRANT SHOW COLUMNS ON ${target} TO ${user_table_grant};
"

echo "Test CREATE TABLE AS an Alias without any privilege on its target"
${CLICKHOUSE_CLIENT} --user="${user_no_grant}" --query "CREATE TABLE ${copy} AS ${alias_table} ENGINE = Memory" 2>&1 | grep -o "ACCESS_DENIED" | uniq
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${copy}"

echo "Test CREATE TABLE AS an Alias with a column-scoped privilege on its target"
${CLICKHOUSE_CLIENT} --user="${user_column_grant}" --query "CREATE TABLE ${copy} AS ${alias_table} ENGINE = Memory" 2>&1 | grep -o "ACCESS_DENIED" | uniq
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${copy}"

echo "Test CREATE TABLE AS an Alias with SHOW COLUMNS on its target"
${CLICKHOUSE_CLIENT} --user="${user_table_grant}" --query "CREATE TABLE ${copy} AS ${alias_table} ENGINE = Memory"
${CLICKHOUSE_CLIENT} --user="${user_table_grant}" --query "DESCRIBE ${copy}" | cut -f 1,2

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE ${copy};
    DROP TABLE ${alias_table};
    DROP TABLE ${target};
    DROP USER ${user_no_grant}, ${user_column_grant}, ${user_table_grant};
"
