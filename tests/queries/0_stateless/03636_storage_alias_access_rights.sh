#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
access_username="access_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP USER IF EXISTS ${access_username};
    DROP TABLE IF EXISTS test_table;
    DROP TABLE IF EXISTS test_alias;
    DROP TABLE IF EXISTS test_alias_access;
    DROP TABLE IF EXISTS test_alias_buffer_access;
    DROP TABLE IF EXISTS test_buffer_access;
    DROP TABLE IF EXISTS test_table_access;

    SET allow_experimental_alias_table_engine = 1;

    CREATE TABLE test_table (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO test_table VALUES (1, 'one'), (2, 'two'), (3, 'three');

    CREATE TABLE test_alias ENGINE = Alias('test_table');
    CREATE USER ${username} NOT IDENTIFIED;
"

# Test: SELECT
echo "Test SELECT without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT * FROM test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

echo "Test INSERT without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "INSERT INTO test_alias VALUES (4, 'four');" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT SELECT ON test_alias TO ${username};
    GRANT SELECT ON test_table TO ${username};
"
echo "Test SELECT with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT count() FROM test_alias;"

# Test: INSERT
echo "Test INSERT still fails"
${CLICKHOUSE_CLIENT} --user="${username}" --query "INSERT INTO test_alias VALUES (4, 'four');" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT INSERT ON test_alias TO ${username};
    GRANT INSERT ON test_table TO ${username};
"
echo "Test INSERT with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "INSERT INTO test_alias VALUES (4, 'four');"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"

# Test: TRUNCATE
echo "Test TRUNCATE without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "TRUNCATE TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT TRUNCATE ON test_alias TO ${username};
    GRANT TRUNCATE ON test_table TO ${username};
"
echo "Test TRUNCATE with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "TRUNCATE TABLE test_alias;"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"

# Test: OPTIMIZE
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_table VALUES (5, 'five');
"
echo "Test OPTIMIZE without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "OPTIMIZE TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT OPTIMIZE ON test_alias TO ${username};
    GRANT OPTIMIZE ON test_table TO ${username};
"
echo "Test OPTIMIZE with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "OPTIMIZE TABLE test_alias FINAL;"

# Test: CHECK TABLE requires CHECK on both the alias and the target table.
echo "Test CHECK TABLE without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "CHECK TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT CHECK ON test_alias TO ${username};"
echo "Test CHECK TABLE with alias permission only"
${CLICKHOUSE_CLIENT} --user="${username}" --query "CHECK TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT CHECK ON test_table TO ${username};"
echo "Test CHECK TABLE with both alias and target permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SET check_query_single_value_result = 1; CHECK TABLE test_alias;"

# Test: ALTER
echo "Test ALTER without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "ALTER TABLE test_alias ADD COLUMN status String DEFAULT 'active';" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT CURRENT GRANTS ON *.* TO ${username};
"

echo "Test ALTER with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "ALTER TABLE test_alias ADD COLUMN status String DEFAULT 'active';"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'test_table' AND name = 'status';"

${CLICKHOUSE_CLIENT} --query "REVOKE DROP ON *.* FROM ${username};"

# Test: DROP alias
echo "Test DROP alias without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "DROP TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT DROP ON test_alias TO ${username};"
echo "Test DROP alias with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "DROP TABLE test_alias;"

# Verify target table still exists
echo "Test target table still exists"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"

# Test target access checks for a newly created `Alias`
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_table_access
    (
        id UInt64,
        value String,
        INDEX value_idx value TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree
    PARTITION BY id % 2
    ORDER BY id
    SAMPLE BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    ALTER TABLE test_table_access MODIFY COMMENT 'target table comment';
    INSERT INTO test_table_access SELECT number + 1, randomString(4096) FROM numbers(2);

    CREATE TABLE test_buffer_access (id UInt64, value String)
    ENGINE = Buffer(currentDatabase(), test_table_access, 1, 1000, 1000, 1000, 1000, 1000000, 1000000);
    INSERT INTO test_buffer_access VALUES (3, 'three');

    CREATE USER ${access_username} NOT IDENTIFIED;
    GRANT CREATE TABLE ON test_alias_access TO ${access_username};
    GRANT CREATE TABLE ON test_alias_buffer_access TO ${access_username};
    GRANT TABLE ENGINE ON Alias TO ${access_username};
    GRANT SELECT ON system.data_skipping_indices TO ${access_username};
"

echo "Test CREATE without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "CREATE TABLE test_alias_access ENGINE = Alias('test_table_access');" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON test_table_access TO ${access_username};"
echo "Test CREATE with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "CREATE TABLE test_alias_access ENGINE = Alias('test_table_access');"

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON test_buffer_access TO ${access_username};"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "CREATE TABLE test_alias_buffer_access ENGINE = Alias('test_buffer_access');"

${CLICKHOUSE_CLIENT} --query "
    GRANT SELECT ON test_alias_access TO ${access_username};
    GRANT SELECT ON test_alias_buffer_access TO ${access_username};
    REVOKE SHOW COLUMNS ON test_table_access FROM ${access_username};
    REVOKE SHOW COLUMNS ON test_buffer_access FROM ${access_username};
"

echo "Test count without target SELECT permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SELECT count() FROM test_alias_access SETTINGS optimize_trivial_count_query = 1;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

echo "Test table statistics without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        isNull(total_rows),
        isNull(total_bytes),
        isNull(total_bytes_uncompressed),
        empty(data_paths),
        empty(storage_policy)
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'test_alias_access';
"

echo "Test table metadata without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        empty(partition_key),
        empty(sorting_key),
        empty(primary_key),
        empty(sampling_key),
        empty(skipping_indices_types),
        empty(comment)
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'test_alias_access';
"

echo "Test column statistics without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT name, data_compressed_bytes > 0
    FROM system.columns
    WHERE database = currentDatabase() AND table = 'test_alias_access'
    ORDER BY name;
"

echo "Test index metadata and statistics without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT count()
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'test_alias_access';
"

echo "Test lifetime statistics without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT isNull(lifetime_rows), isNull(lifetime_bytes)
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'test_alias_buffer_access';
"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT(value) ON test_table_access TO ${access_username};"

echo "Test direct and Alias count with column-scoped target SELECT permission using the analyzer"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        (SELECT count() FROM test_table_access),
        (SELECT count() FROM test_alias_access)
    SETTINGS optimize_trivial_count_query = 1, enable_analyzer = 1;
"

echo "Test table statistics with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        total_rows,
        total_bytes > 0,
        total_bytes_uncompressed > 0,
        notEmpty(data_paths),
        notEmpty(storage_policy)
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'test_alias_access';
"

echo "Test table metadata with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        notEmpty(partition_key),
        notEmpty(sorting_key),
        notEmpty(primary_key),
        notEmpty(sampling_key),
        notEmpty(skipping_indices_types),
        notEmpty(comment)
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'test_alias_access';
"

echo "Test column statistics with column-scoped target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT name, data_compressed_bytes > 0
    FROM system.columns
    WHERE database = currentDatabase() AND table = 'test_alias_access'
    ORDER BY name;
"

echo "Test index metadata and statistics with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        count(),
        countIf(notEmpty(name) AND notEmpty(type) AND notEmpty(expr) AND granularity > 0),
        sum(data_compressed_bytes + data_uncompressed_bytes + marks_bytes) > 0
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'test_alias_access';
"

${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON test_buffer_access TO ${access_username};"
echo "Test lifetime statistics with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT lifetime_rows, lifetime_bytes > 0
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'test_alias_buffer_access';
"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE test_alias_buffer_access;
    DROP TABLE test_alias_access;
    DROP TABLE test_buffer_access;
    DROP TABLE test_table_access;
    DROP USER ${access_username};
"
