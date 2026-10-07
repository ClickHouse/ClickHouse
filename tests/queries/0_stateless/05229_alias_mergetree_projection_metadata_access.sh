#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `mergeTreeProjection` table function forwards the source table's metadata, so for an `Alias`
# source it used to expose the target's projection columns to a user with no grant on the target,
# while `DESCRIBE` of the alias itself already denies it. The table function must apply the same
# target-side `SHOW COLUMNS` check.

access_username="access_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${access_username};
    DROP TABLE IF EXISTS test_alias_proj_access;
    DROP TABLE IF EXISTS test_table_proj_access;

    CREATE TABLE test_table_proj_access
    (
        id UInt64,
        secret String,
        PROJECTION secret_projection (SELECT secret ORDER BY secret)
    )
    ENGINE = MergeTree
    ORDER BY id;
    INSERT INTO test_table_proj_access SELECT number, randomString(8) FROM numbers(2);

    CREATE TABLE test_alias_proj_access ENGINE = Alias('test_table_proj_access');

    CREATE USER ${access_username} NOT IDENTIFIED;
    GRANT SELECT, SHOW COLUMNS ON test_alias_proj_access TO ${access_username};
"

echo "Test mergeTreeProjection metadata without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "DESCRIBE mergeTreeProjection(currentDatabase(), test_alias_proj_access, secret_projection);" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "Test mergeTreeProjection read without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SELECT secret FROM mergeTreeProjection(currentDatabase(), test_alias_proj_access, secret_projection) FORMAT Null;" 2>&1 | grep -o "ACCESS_DENIED" | head -1

${CLICKHOUSE_CLIENT} --query "GRANT SELECT, SHOW COLUMNS ON test_table_proj_access TO ${access_username};"

echo "Test mergeTreeProjection metadata with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "DESCRIBE mergeTreeProjection(currentDatabase(), test_alias_proj_access, secret_projection);" | cut -f1

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER ${access_username};
    DROP TABLE test_alias_proj_access;
    DROP TABLE test_table_proj_access;
"
