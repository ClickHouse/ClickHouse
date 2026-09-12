#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

access_username="access_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Test target access checks for a newly created `Alias`
${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${access_username};
    DROP TABLE IF EXISTS test_alias_access;
    DROP TABLE IF EXISTS test_alias_buffer_access;
    DROP TABLE IF EXISTS test_buffer_access;
    DROP TABLE IF EXISTS test_table_access;

    CREATE TABLE test_table_access
    (
        id UInt64,
        value String,
        CONSTRAINT value_not_empty CHECK notEmpty(value),
        PROJECTION value_projection (SELECT value ORDER BY value),
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
    GRANT SELECT ON system.completions TO ${access_username};
    GRANT SELECT ON system.constraints TO ${access_username};
    GRANT SELECT ON system.data_skipping_indices TO ${access_username};
    GRANT SELECT ON system.projections TO ${access_username};
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

    DETACH TABLE test_alias_access;
    ATTACH TABLE test_alias_access;
"

echo "Test count without target SELECT permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SELECT count() FROM test_alias_access SETTINGS optimize_trivial_count_query = 1;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

echo "Test count without target SELECT permission using the read path"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SELECT count() FROM test_alias_access SETTINGS optimize_trivial_count_query = 0, enable_analyzer = 1;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

echo "Test count without target SELECT permission using the old analyzer read path"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SELECT count() FROM test_alias_access SETTINGS optimize_trivial_count_query = 0, enable_analyzer = 0;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

echo "Test DESCRIBE without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "DESCRIBE TABLE test_alias_access;" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "Test SHOW CREATE without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SHOW CREATE TABLE test_alias_access;" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "Test SHOW COLUMNS without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SHOW COLUMNS FROM test_alias_access;"

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

echo "Test persisted table metadata without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT empty(create_table_query), empty(engine_full)
    FROM system.tables
    WHERE database = currentDatabase() AND name = 'test_alias_access';
"

echo "Test column statistics without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT count()
    FROM system.columns
    WHERE database = currentDatabase() AND table = 'test_alias_access';
"

echo "Test other metadata tables without target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        (SELECT count() FROM system.constraints WHERE database = currentDatabase() AND table = 'test_alias_access'),
        (SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 'test_alias_access'),
        (SELECT count() FROM system.completions WHERE context = 'column' AND belongs = 'test_alias_access');
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

echo "Test direct and Alias count with column-scoped target SELECT permission using the read path"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        (SELECT count() FROM test_table_access),
        (SELECT count() FROM test_alias_access)
    SETTINGS optimize_trivial_count_query = 0, enable_analyzer = 1;
"

echo "Test direct and Alias count with column-scoped target SELECT permission using the old analyzer read path"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        (SELECT count() FROM test_table_access),
        (SELECT count() FROM test_alias_access)
    SETTINGS optimize_trivial_count_query = 0, enable_analyzer = 0;
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

echo "Test persisted table metadata with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT notEmpty(create_table_query), notEmpty(engine_full)
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

echo "Test other metadata tables with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "
    SELECT
        (SELECT count() FROM system.constraints WHERE database = currentDatabase() AND table = 'test_alias_access'),
        (SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 'test_alias_access'),
        (SELECT count() FROM system.completions WHERE context = 'column' AND belongs = 'test_alias_access');
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

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON test_table_access TO ${access_username};"
echo "Test schema commands with target permission"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SELECT arraySort(groupArray(name)) FROM system.columns WHERE database = currentDatabase() AND table = 'test_alias_access';"
${CLICKHOUSE_CLIENT} --user="${access_username}" --multiquery --query "DESCRIBE TABLE test_alias_access FORMAT Null; SELECT 'DESCRIBE OK';"
${CLICKHOUSE_CLIENT} --user="${access_username}" --multiquery --query "SHOW COLUMNS FROM test_alias_access FORMAT Null; SELECT 'SHOW COLUMNS OK';"
${CLICKHOUSE_CLIENT} --user="${access_username}" --query "SHOW CREATE TABLE test_alias_access FORMAT TSVRaw;" | grep -o "ENGINE = Alias" | uniq

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE test_alias_buffer_access;
    DROP TABLE test_alias_access;
    DROP TABLE test_buffer_access;
    DROP TABLE test_table_access;
    DROP USER ${access_username};
"
