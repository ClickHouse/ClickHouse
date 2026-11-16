#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: on a replicated database the DDL runs with no user, so the access check asserted here is skipped and the deny path silently allows, https://github.com/ClickHouse/ClickHouse/issues/111561

# Creating an `ABR` table copies the columns of its source tables, so it requires
# `SHOW COLUMNS` on every one of them, checked before anything about them is revealed.
# The engine arguments are evaluated with the privileges of the user, so a subquery in them cannot read other tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS abr_proxy;
    DROP TABLE IF EXISTS src_1;
    DROP TABLE IF EXISTS src_10;
    DROP TABLE IF EXISTS secrets;

    CREATE TABLE src_1 (ts DateTime, account_id String, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    CREATE TABLE src_10 (ts DateTime, account_id String, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    CREATE TABLE secrets (s String) ENGINE = Memory;
    INSERT INTO secrets VALUES ('xyzzy');

    CREATE USER ${user} NOT IDENTIFIED;
    GRANT CREATE TABLE ON abr_proxy TO ${user};
    GRANT TABLE ENGINE ON ABR TO ${user};
"

echo "CREATE without privileges on the source tables"
${CLICKHOUSE_CLIENT} --user="${user}" --allow_experimental_abr=1 --query "CREATE TABLE abr_proxy ENGINE = ABR('src', [1, 10], ('ts'))" 2>&1 | grep -o "ACCESS_DENIED" | uniq

echo "CREATE over source tables that do not exist"
${CLICKHOUSE_CLIENT} --user="${user}" --allow_experimental_abr=1 --query "CREATE TABLE abr_proxy ENGINE = ABR('missing', [1, 10], ('ts'))" 2>&1 | grep -o "ACCESS_DENIED" | uniq

echo "CREATE with a subquery over a table the user cannot read in the arguments"
${CLICKHOUSE_CLIENT} --user="${user}" --allow_experimental_abr=1 --query "CREATE TABLE abr_proxy ENGINE = ABR((SELECT any(s) FROM ${CLICKHOUSE_DATABASE}.secrets), [1, 10], ('ts'))" 2>&1 | grep -o -e "ACCESS_DENIED" -e "xyzzy" | sort -u

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON src_1 TO ${user}"
echo "CREATE with privileges on only some of the source tables"
${CLICKHOUSE_CLIENT} --user="${user}" --allow_experimental_abr=1 --query "CREATE TABLE abr_proxy ENGINE = ABR('src', [1, 10], ('ts'))" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'abr_proxy'"

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON src_10 TO ${user}"
echo "CREATE with privileges on all the source tables"
${CLICKHOUSE_CLIENT} --user="${user}" --allow_experimental_abr=1 --query "CREATE TABLE abr_proxy ENGINE = ABR('src', [1, 10], ('ts'))"
${CLICKHOUSE_CLIENT} --query "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'abr_proxy' ORDER BY position"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE abr_proxy;
    DROP TABLE src_1;
    DROP TABLE src_10;
    DROP TABLE secrets;
    DROP USER ${user};
"
